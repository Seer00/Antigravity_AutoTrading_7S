# -*- coding: utf-8 -*-
"""
자동매매 트리거 엔진
실시간 체결가를 감시하여 종목별 세븐스플릿 로직(분할매수/분할매도)을 자동 실행합니다.
실제 키움 클라이언트 또는 Mock 클라이언트를 주입받아 동작합니다.
"""

import asyncio
import json
import logging
from datetime import datetime
from database import SessionLocal, SplitConfig, SplitState, OrderLog, EmergencyLog

logger = logging.getLogger("TriggerEngine")

# 대시보드로 실시간 로그를 전달하기 위한 전역 큐
system_log_queue = asyncio.Queue()
execution_log_queue = asyncio.Queue()

def log_system_event(message: str, level: str = "INFO"):
    """
    이벤트를 표준 로거에 기록하고 대시보드 스트리밍 큐에 적재합니다.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] [{level}] {message}"
    
    if level == "INFO":
        logger.info(message)
    elif level == "WARNING":
        logger.warning(message)
    elif level == "ERROR":
        logger.error(message)
        
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(system_log_queue.put_nowait, formatted_msg)
    except RuntimeError:
        pass


def log_execution_event(time_str: str, stock_name: str, order_type: str, price: float, qty: float, note: str = "자동"):
    """
    매수/매도 체결 발생 시 체결 전용 큐에 데이터를 적재합니다.
    """
    exec_data = {
        "time": time_str,
        "stock_name": stock_name,
        "order_type": "매수" if order_type == "BUY" else "매도",
        "price": price,
        "qty": qty,
        "note": note
    }
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(execution_log_queue.put_nowait, exec_data)
    except RuntimeError:
        pass


class TriggerEngine:
    """
    세븐스플릿 자동 거래 트리거 엔진
    """
    def __init__(self, kiwoom_client):
        self.kiwoom = kiwoom_client
        # 종목코드별 처리 중 락 (동시성 방지)
        self.locks = {}
        # 현재 감시 중인 종목 리스트
        self.monitored_stocks = set()
        
        # 키움 클라이언트에 시세 수신 시 실행할 콜백 등록
        self.kiwoom.register_price_callback(self.on_price_update)

    def _get_lock(self, stock_code: str) -> asyncio.Lock:
        if stock_code not in self.locks:
            self.locks[stock_code] = asyncio.Lock()
        return self.locks[stock_code]

    async def reload_configs(self):
        """
        DB에서 활성화된 종목 설정들을 로드하여 감시 목록을 갱신하고
        필요 시 웹소켓을 재연결합니다.
        """
        db = SessionLocal()
        try:
            active_configs = db.query(SplitConfig).filter(SplitConfig.is_active == True).all()
            new_stocks = {c.stock_code for c in active_configs}
            
            # 감시 종목이 바뀌었거나 신규 등록된 경우
            if new_stocks != self.monitored_stocks:
                self.monitored_stocks = new_stocks
                log_system_event(f"감시 종목 목록이 갱신되었습니다: {list(self.monitored_stocks)}", "INFO")
                
                # 키움 클라이언트가 구동 중인 경우 실시간 시세 재등록
                # WebSocket 수신 재시작
                if self.monitored_stocks:
                    if hasattr(self.kiwoom, "is_running") and self.kiwoom.is_running:
                        # Mock 클라이언트인 경우 기존 태스크가 돌고 있으면 정지 후 재시작
                        await self.kiwoom.close()
                        await self.kiwoom.start_websocket(list(self.monitored_stocks))
                    elif hasattr(self.kiwoom, "ws_client") and self.kiwoom.ws_client:
                        # 실제 키움 클라이언트 WebSocket 정지 후 재시작
                        await self.kiwoom.close()
                        await self.kiwoom.start_websocket(list(self.monitored_stocks))
        finally:
            db.close()

    async def start(self):
        """
        엔진을 시작하고 최초 감시 대상 종목들의 WebSocket을 구동합니다.
        """
        log_system_event("세븐스플릿 자동투자 엔진 시동 중...", "INFO")
        db = SessionLocal()
        try:
            # 설정된 종목 중 감시 활성화된 종목을 DB에서 읽어옴
            configs = db.query(SplitConfig).filter(SplitConfig.is_active == True).all()
            for config in configs:
                # 활성화된 종목들의 단계 상태가 DB에 생성되어 있지 않다면 7단계(혹은 N단계) 상태 기본 셋업
                existing_states = db.query(SplitState).filter(SplitState.config_id == config.id).count()
                if existing_states == 0:
                    log_system_event(f"종목 {config.stock_name}({config.stock_code})의 스플릿 단계 상태 정보를 생성합니다.", "INFO")
                    for i in range(1, config.total_steps + 1):
                        state = SplitState(
                            config_id=config.id,
                            step=i,
                            status="WAIT",
                            buy_price=0.0,
                            quantity=0.0
                        )
                        db.add(state)
                    db.commit()
            
            self.monitored_stocks = {c.stock_code for c in configs}
            
            if self.monitored_stocks:
                log_system_event(f"실시간 시세 감시를 시작합니다. 대상 종목: {list(self.monitored_stocks)}", "INFO")
                await self.kiwoom.start_websocket(list(self.monitored_stocks))
            else:
                log_system_event("감시 대상 종목이 없습니다. 대시보드에서 종목 설정을 추가하세요.", "WARNING")
        except Exception as e:
            log_system_event(f"엔진 구동 중 오류 발생: {str(e)}", "ERROR")
        finally:
            db.close()

    async def stop(self):
        """
        엔진 및 클라이언트 연결 종료
        """
        log_system_event("세븐스플릿 자동투자 엔진 종료 중...", "INFO")
        await self.kiwoom.close()

    async def on_price_update(self, stock_code: str, current_price: float):
        """
        실시간 시세가 들어올 때 호출되는 콜백 핸들러
        세븐스플릿 매매 로직의 핵심이 작동하는 지점입니다.
        """
        # 한 종목에 대해 여러 시세 업데이트가 겹치지 않도록 락 획득
        lock = self._get_lock(stock_code)
        if lock.locked():
            return  # 현재 이전 시세 이벤트 처리 중이면 건너뜀 (슬리피지 방지 및 속도 제어)

        async with lock:
            db = SessionLocal()
            try:
                # 종목 설정 조회
                config = db.query(SplitConfig).filter(
                    SplitConfig.stock_code == stock_code,
                    SplitConfig.is_active == True
                ).first()
                if not config:
                    return

                # 수동매매 모드인 경우 자동 트리거 매수/매도를 실행하지 않음
                if getattr(config, "trade_mode", "AUTO") == "MANUAL":
                    return

                # 이 종목의 1~N단계 상태 데이터 로드
                states = db.query(SplitState).filter(SplitState.config_id == config.id).order_by(SplitState.step).all()
                if not states:
                    return

                # 차수별 세부 설정(step_settings) 파싱
                step_map = {}
                if getattr(config, "step_settings", None):
                    try:
                        parsed = json.loads(config.step_settings)
                        for item in parsed:
                            st = item.get("step")
                            if st:
                                step_map[st] = item
                    except Exception as pe:
                        logger.error(f"step_settings 파싱 오류: {pe}")

                # ----------------------------------------------------
                # [로직 1] 매도 감시
                # 보유(HOLD) 상태인 모든 단계를 검사하여 목표수익률 도달 시 매도
                # ----------------------------------------------------
                for state in states:
                    if state.status == "HOLD":
                        buy_price = state.buy_price
                        qty = state.quantity
                        
                        # 수익률 계산
                        profit_rate = ((current_price - buy_price) / buy_price) * 100
                        
                        # 해당 차수 전용 목표수익률 (설정 없으면 기본 target_profit_pct)
                        st_cfg = step_map.get(state.step, {})
                        target_profit = float(st_cfg.get("profit_pct", config.target_profit_pct))

                        if profit_rate >= target_profit:
                            log_system_event(
                                f"[트리거 발생] {config.stock_name}({stock_code}) {state.step}단계 목표 수익률 도달! "
                                f"(매수가: {buy_price}원 -> 현재가: {current_price}원, 수익률: {profit_rate:.2f}%, 목표: {target_profit:.1f}%)",
                                "INFO"
                            )
                            
                            # 매도 주문 전송
                            order_res = await self.kiwoom.place_order(stock_code, "SELL", 0, int(qty))
                            if order_res.get("success"):
                                exec_price = order_res.get("executed_price", current_price)
                                exec_qty = order_res.get("executed_quantity", qty)
                                
                                state.status = "SOLD"
                                
                                order_log = OrderLog(
                                    config_id=config.id,
                                    step=state.step,
                                    order_type="SELL",
                                    order_price=current_price,
                                    order_quantity=qty,
                                    executed_price=exec_price,
                                    executed_quantity=exec_qty,
                                    status="FILLED",
                                    order_time=datetime.now()
                                )
                                db.add(order_log)
                                
                                now_str = datetime.now().strftime("%H:%M:%S")
                                log_system_event(
                                    f"[매도 체결 완료] {config.stock_name} {state.step}단계 매도 체결! "
                                    f"수량: {exec_qty}주, 단가: {exec_price}원",
                                    "INFO"
                                )
                                log_execution_event(now_str, config.stock_name, "SELL", exec_price, exec_qty, "자동")
                                
                                if config.reinvest_enabled:
                                    state.status = "WAIT"
                                    state.buy_price = 0.0
                                    state.quantity = 0.0
                                    state.buy_time = None
                                    log_system_event(f"{config.stock_name} {state.step}단계 다시 매수 대기 상태로 초기화되었습니다.", "INFO")
                                
                                db.commit()
                            else:
                                log_system_event(
                                    f"[매도 실패] {config.stock_name} {state.step}단계 주문 실패: {order_res.get('message')}",
                                    "ERROR"
                                )

                # ----------------------------------------------------
                # [로직 2] 매수 감시
                # ----------------------------------------------------
                
                # 1단계가 대기(WAIT) 상태인 경우 -> 즉시 진입
                first_step = states[0]
                if first_step.status == "WAIT":
                    st_cfg = step_map.get(1, {})
                    entry_amt = float(st_cfg.get("buy_amount", config.first_entry_amount))

                    log_system_event(
                        f"[트리거 발생] {config.stock_name}({stock_code}) 1단계 최초 진입 시도. 현재가: {current_price}원 (투입금액: {entry_amt:,.0f}원)",
                        "INFO"
                    )
                    
                    buy_qty = int(entry_amt // current_price)
                    if buy_qty <= 0:
                        log_system_event(
                            f"[진입 실패] 1단계 투입금액({entry_amt}원)이 현재 주가({current_price}원)보다 적어 1주도 살 수 없습니다.",
                            "WARNING"
                        )
                        return
                    
                    order_res = await self.kiwoom.place_order(stock_code, "BUY", 0, buy_qty)
                    if order_res.get("success"):
                        exec_price = order_res.get("executed_price", current_price)
                        exec_qty = order_res.get("executed_quantity", buy_qty)
                        
                        first_step.status = "HOLD"
                        first_step.buy_price = exec_price
                        first_step.quantity = exec_qty
                        first_step.buy_time = datetime.now()
                        
                        order_log = OrderLog(
                            config_id=config.id,
                            step=1,
                            order_type="BUY",
                            order_price=current_price,
                            order_quantity=buy_qty,
                            executed_price=exec_price,
                            executed_quantity=exec_qty,
                            status="FILLED",
                            order_time=datetime.now()
                        )
                        db.add(order_log)
                        db.commit()
                        
                        now_str = datetime.now().strftime("%H:%M:%S")
                        log_system_event(
                            f"[매수 체결 완료] {config.stock_name} 1단계 진입 성공! "
                            f"수량: {exec_qty}주, 단가: {exec_price}원 (총액: {exec_price * exec_qty:,.0f}원)",
                            "INFO"
                        )
                        log_execution_event(now_str, config.stock_name, "BUY", exec_price, exec_qty, "자동")
                    else:
                        log_system_event(f"[매수 실패] {config.stock_name} 1단계 주문 실패: {order_res.get('message')}", "ERROR")
                    return

                # 1단계가 이미 HOLD 상태인 경우, 추가 하락 분할 매수 감시
                highest_hold_step = None
                for state in states:
                    if state.status == "HOLD":
                        highest_hold_step = state

                if highest_hold_step:
                    current_step_num = highest_hold_step.step
                    
                    if current_step_num < config.total_steps:
                        next_step = states[current_step_num] # index = step
                        
                        if next_step.status == "WAIT":
                            prev_buy_price = highest_hold_step.buy_price
                            drop_rate = ((current_price - prev_buy_price) / prev_buy_price) * 100
                            
                            next_st_cfg = step_map.get(next_step.step, {})
                            drop_trigger = float(next_st_cfg.get("drop_pct", config.drop_trigger_pct))
                            entry_amt = float(next_st_cfg.get("buy_amount", config.first_entry_amount))

                            if drop_rate <= -drop_trigger:
                                log_system_event(
                                    f"[트리거 발생] {config.stock_name}({stock_code}) {next_step.step}단계 추가 매수 하락 트리거 도달! "
                                    f"(직전 {current_step_num}단계 매수가: {prev_buy_price}원 -> 현재가: {current_price}원, 등락률: {drop_rate:.2f}%, 기준: -{drop_trigger:.1f}%)",
                                    "INFO"
                                )
                                
                                buy_qty = int(entry_amt // current_price)
                                if buy_qty <= 0:
                                    log_system_event(
                                        f"[추가매수 실패] {next_step.step}단계 투입금액이 주가보다 적어 1주도 살 수 없습니다.",
                                        "WARNING"
                                    )
                                    return
                                    
                                order_res = await self.kiwoom.place_order(stock_code, "BUY", 0, buy_qty)
                                if order_res.get("success"):
                                    exec_price = order_res.get("executed_price", current_price)
                                    exec_qty = order_res.get("executed_quantity", buy_qty)
                                    
                                    next_step.status = "HOLD"
                                    next_step.buy_price = exec_price
                                    next_step.quantity = exec_qty
                                    next_step.buy_time = datetime.now()
                                    
                                    order_log = OrderLog(
                                        config_id=config.id,
                                        step=next_step.step,
                                        order_type="BUY",
                                        order_price=current_price,
                                        order_quantity=buy_qty,
                                        executed_price=exec_price,
                                        executed_quantity=exec_qty,
                                        status="FILLED",
                                        order_time=datetime.now()
                                    )
                                    db.add(order_log)
                                    db.commit()
                                    
                                    now_str = datetime.now().strftime("%H:%M:%S")
                                    log_system_event(
                                        f"[매수 체결 완료] {config.stock_name} {next_step.step}단계 추가 진입 완료! "
                                        f"수량: {exec_qty}주, 단가: {exec_price}원 (총액: {exec_price * exec_qty:,.0f}원)",
                                        "INFO"
                                    )
                                    log_execution_event(now_str, config.stock_name, "BUY", exec_price, exec_qty, "자동")
                                else:
                                    log_system_event(
                                        f"[매수 실패] {config.stock_name} {next_step.step}단계 주문 실패: {order_res.get('message')}",
                                        "ERROR"
                                    )

            except Exception as e:
                log_system_event(f"시세 업데이트 로직 처리 중 오류 발생 ({stock_code}): {str(e)}", "ERROR")
                db.rollback()
            finally:
                db.close()

    async def execute_emergency_liquidation(self, config_id: int = None) -> bool:
        """
        특정 종목 또는 보유 전 종목을 즉시 전량 매도(긴급 청산)합니다.
        Trigger Engine 감시 상태와 무관하게 즉시 시장가 매도로 실행됩니다.
        config_id가 None이면 전 종목 청산, 지정되어 있으면 특정 종목만 청산합니다.
        """
        db = SessionLocal()
        try:
            if config_id:
                configs = db.query(SplitConfig).filter(SplitConfig.id == config_id).all()
            else:
                configs = db.query(SplitConfig).filter(SplitConfig.is_active == True).all()

            if not configs:
                log_system_event("긴급 청산 대상 종목이 없습니다.", "WARNING")
                return False

            success_flag = True
            for config in configs:
                # 해당 종목의 HOLD 상태인 스플릿 단계들 조회
                holding_states = db.query(SplitState).filter(
                    SplitState.config_id == config.id,
                    SplitState.status == "HOLD"
                ).all()

                if not holding_states:
                    log_system_event(f"종목 {config.stock_name}({config.stock_code})은 보유 중인 스플릿 단계가 없어 청산을 건너뜁니다.", "INFO")
                    continue

                total_qty = sum(s.quantity for s in holding_states)
                log_system_event(f"[긴급 청산 시작] {config.stock_name}({config.stock_code}) 총 {total_qty}주 즉시 청산(매도) 요청", "WARNING")

                # 일괄 매도 주문 (시장가)
                order_res = await self.kiwoom.place_order(config.stock_code, "SELL", 0, int(total_qty))
                if order_res.get("success"):
                    exec_price = order_res.get("executed_price", 0.0)
                    exec_qty = order_res.get("executed_quantity", total_qty)

                    # 관련 상태 전체 초기화
                    for state in holding_states:
                        state.status = "WAIT"
                        state.buy_price = 0.0
                        state.quantity = 0.0
                        state.buy_time = None
                    
                    # 긴급 청산 로그 생성
                    emergency_log = EmergencyLog(
                        stock_code=config.stock_code,
                        stock_name=config.stock_name,
                        quantity=exec_qty,
                        reason="사용자 수동 긴급 청산 요청",
                        executed_time=datetime.now()
                    )
                    db.add(emergency_log)

                    # 주문 로그 생성
                    order_log = OrderLog(
                        config_id=config.id,
                        step=None,  # 긴급 청산은 특정 단계가 아닌 일괄 청산이므로 None
                        order_type="SELL",
                        order_price=0.0,
                        order_quantity=total_qty,
                        executed_price=exec_price,
                        executed_quantity=exec_qty,
                        status="FILLED",
                        order_time=datetime.now()
                    )
                    db.add(order_log)
                    
                    now_str = datetime.now().strftime("%H:%M:%S")
                    log_system_event(
                        f"[긴급 청산 완료] {config.stock_name} 전량 청산 완료! "
                        f"수량: {exec_qty}주, 평균 체결가: {exec_price}원",
                        "WARNING"
                    )
                    log_execution_event(now_str, config.stock_name, "SELL", exec_price, exec_qty, "수동")
                else:
                    log_system_event(f"[긴급 청산 실패] {config.stock_name} 주문 전송 실패: {order_res.get('message')}", "ERROR")
                    success_flag = False

            db.commit()
            
            # 감시 종목 상태 갱신을 위해 클라이언트 websocket 등 갱신
            await self.reload_configs()
            return success_flag

        except Exception as e:
            log_system_event(f"긴급 청산 수행 중 예외 발생: {str(e)}", "ERROR")
            db.rollback()
            return False
        finally:
            db.close()

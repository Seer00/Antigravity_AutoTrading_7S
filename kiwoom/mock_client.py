# -*- coding: utf-8 -*-
"""
키움증권 REST 및 WebSocket API 모의 클라이언트 (시뮬레이터)
실제 API 키나 인증서 없이도 7스플릿 로직이 완벽하게 돌아가도록 로컬 시뮬레이션 환경을 구축합니다.
가상 계좌 상태 관리 및 백그라운드 가상 시세 변동을 처리합니다.
"""

import asyncio
import random
import logging
from datetime import datetime
from database import SessionLocal, SplitConfig, SplitState

logger = logging.getLogger("KiwoomMockClient")

class KiwoomMockClient:
    """
    키움증권 API Mock / 시뮬레이션 클라이언트
    """
    def __init__(self, app_key: str = None, secret_key: str = None, account_no: str = None, is_mock: bool = True):
        self.app_key = app_key or "MOCK_APP_KEY"
        self.secret_key = secret_key or "MOCK_SECRET_KEY"
        self.account_no = account_no or "8888-8888-88"
        self.is_mock = True  # Mock 클라이언트는 항상 모의 모드

        # 가상 자산 관리 (기본 가상 예수금 5천만원)
        self.mock_deposit = 50000000.0
        
        # 종목코드별 가상 현재가 관리
        self.mock_prices = {}
        # 종목별 주가 변동 방향 시나리오 설정 ('UP', 'DOWN', 'SIDE', 'RANDOM')
        self.scenarios = {}

        self.price_callbacks = []
        self.sim_task = None
        self.is_running = False

    async def authenticate(self) -> bool:
        """
        가상 인증 성공 처리
        """
        logger.info("[MOCK] 가상 계좌 인증에 성공하였습니다. (계좌번호: %s)", self.account_no)
        return True

    async def get_balance(self) -> dict:
        """
        데이터베이스의 split_state 정보를 동적으로 취합하여 가상의 계좌 잔고를 반환합니다.
        """
        db = SessionLocal()
        try:
            # 활성화된 설정 종목들을 조회
            configs = db.query(SplitConfig).filter(SplitConfig.is_active == True).all()
            holdings = []
            total_stock_eval = 0.0

            for config in configs:
                # 해당 종목 중 현재 보유(HOLD) 상태인 단계들 취합
                holding_states = db.query(SplitState).filter(
                    SplitState.config_id == config.id,
                    SplitState.status == "HOLD"
                ).all()

                if not holding_states:
                    continue

                # 총 수량, 총 매수금액 계산
                total_qty = sum(s.quantity for s in holding_states)
                total_buy_amt = sum(s.quantity * s.buy_price for s in holding_states)
                avg_price = total_buy_amt / total_qty if total_qty > 0 else 0.0
                
                # 현재가 가져오기
                current_price = self.mock_prices.get(config.stock_code, avg_price)
                if current_price == 0.0:
                    current_price = avg_price

                eval_amt = total_qty * current_price
                eval_profit = eval_amt - total_buy_amt
                profit_rate = (eval_profit / total_buy_amt * 100) if total_buy_amt > 0 else 0.0

                total_stock_eval += eval_amt

                holdings.append({
                    "stock_code": config.stock_code,
                    "stock_name": config.stock_name,
                    "quantity": total_qty,
                    "buy_price": round(avg_price, 2),
                    "current_price": current_price,
                    "eval_profit": round(eval_profit, 2),
                    "profit_rate": round(profit_rate, 2)
                })

            # 총 자산 = 가상 예수금 + 보유주식 평가금액
            total_asset = self.mock_deposit + total_stock_eval

            return {
                "success": True,
                "deposit": self.mock_deposit,
                "total_asset": total_asset,
                "holdings": holdings
            }
        except Exception as e:
            logger.error("[MOCK] 잔고 조회 중 오류 발생: %s", str(e))
            return {"success": False, "message": str(e)}
        finally:
            db.close()

    async def get_stock_price(self, stock_code: str) -> float:
        """
        가상 현재가 반환
        """
        price = self.mock_prices.get(stock_code, 50000.0)
        return price

    async def place_order(self, stock_code: str, order_type: str, price: float, quantity: int) -> dict:
        """
        모의 주문 처리. 가상의 주문 체결을 로컬에서 즉시 시뮬레이션합니다.
        시장가 주문 시 현재 mock 가격으로 즉시 체결 성공을 처리합니다.
        """
        current_price = self.mock_prices.get(stock_code, price if price > 0 else 50000.0)
        executed_price = current_price if price == 0 else price
        order_amt = executed_price * quantity

        # 가상 주문 정보 로깅
        order_no = f"M{random.randint(100000, 999999)}"
        logger.info("[MOCK 주문] 유형: %s, 종목: %s, 단가: %s, 수량: %s, 금액: %s, 주문번호: %s",
                    order_type, stock_code, executed_price, quantity, order_amt, order_no)

        # 가상 예수금 반영
        if order_type == "BUY":
            # 실제 주문이라면 예수금이 차감됨
            self.mock_deposit -= order_amt
        else:
            # 매도라면 예수금이 증액됨
            self.mock_deposit += order_amt

        return {
            "success": True,
            "order_no": order_no,
            "executed_price": executed_price,
            "executed_quantity": quantity
        }

    def register_price_callback(self, callback):
        """
        실시간 가상 시세 수신 시 트리거할 콜백 등록
        """
        self.price_callbacks.append(callback)

    def set_price_scenario(self, stock_code: str, scenario: str):
        """
        종목별 주가 시나리오 방향을 설정합니다.
        scenario: 'UP' (우상향), 'DOWN' (우하향), 'SIDE' (횡보), 'RANDOM' (무작위)
        """
        self.scenarios[stock_code] = scenario
        logger.info("[MOCK] 종목 %s의 가격 변동 시나리오가 '%s'로 변경되었습니다.", stock_code, scenario)

    async def start_websocket(self, stock_codes: list[str]):
        """
        로컬 가상 주가 생성 시뮬레이터를 백그라운드 태스크로 작동시킵니다. (WebSocket 역할 모사)
        """
        self.is_running = True
        
        # 등록된 종목들의 최초 가격이 설정되어 있지 않다면 임의로 초기화
        for code in stock_codes:
            if code not in self.mock_prices:
                # 대략적인 국내 주식 가격 셋업 (끝자리 00으로 깔끔하게 매칭)
                self.mock_prices[code] = float(random.randint(100, 1500) * 100)
            if code not in self.scenarios:
                self.scenarios[code] = 'RANDOM'

        async def simulation_loop():
            logger.info("[MOCK] 가상 시세 시뮬레이터가 구동되었습니다. (감시 종목: %s)", stock_codes)
            while self.is_running:
                try:
                    for code in stock_codes:
                        current_price = self.mock_prices.get(code, 50000.0)
                        scenario = self.scenarios.get(code, 'RANDOM')

                        # 시나리오에 따른 주가 등락률 계산
                        if scenario == 'UP':
                            # 평균 상승 편향 (+0.1% ~ +1.0%)
                            change_rate = random.uniform(-0.2, 0.8) / 100
                        elif scenario == 'DOWN':
                            # 평균 하락 편향 (-1.0% ~ +0.2%)
                            change_rate = random.uniform(-0.8, 0.2) / 100
                        elif scenario == 'SIDE':
                            # 좁은 횡보 (-0.3% ~ +0.3%)
                            change_rate = random.uniform(-0.3, 0.3) / 100
                        else:
                            # 완전 무작위 등락 (-1.0% ~ +1.0%)
                            change_rate = random.uniform(-1.0, 1.0) / 100

                        # 새 가격 책정 및 호가 단위(틱) 맞춰 버림/올림 처리
                        new_price = current_price * (1 + change_rate)
                        
                        # 틱 단위 보정 (대략적인 호가 틱)
                        if new_price >= 500000: tick = 1000
                        elif new_price >= 100000: tick = 500
                        elif new_price >= 50000: tick = 100
                        elif new_price >= 10000: tick = 50
                        elif new_price >= 5000: tick = 10
                        elif new_price >= 1000: tick = 5
                        else: tick = 1

                        new_price = float(round(new_price / tick) * tick)
                        
                        # 최하 가격 방어
                        if new_price < 100.0:
                            new_price = 100.0

                        self.mock_prices[code] = new_price

                        # 등록된 콜백 함수들에게 가격 정보 브로드캐스트
                        for cb in self.price_callbacks:
                            if asyncio.iscoroutinefunction(cb):
                                await cb(code, new_price)
                            else:
                                cb(code, new_price)

                    # 1초마다 시세 갱신
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    logger.info("[MOCK] 가상 시세 시뮬레이터 태스크 취소")
                    break
                except Exception as e:
                    logger.error("[MOCK] 시뮬레이션 루프 오류: %s", str(e))
                    await asyncio.sleep(2.0)

        self.sim_task = asyncio.create_task(simulation_loop())

    async def close(self):
        """
        시뮬레이터 정지
        """
        self.is_running = False
        if self.sim_task:
            self.sim_task.cancel()
            try:
                await self.sim_task
            except asyncio.CancelledError:
                pass
        logger.info("[MOCK] 가상 시뮬레이터가 종료되었습니다.")

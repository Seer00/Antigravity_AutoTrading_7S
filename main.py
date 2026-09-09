# -*- coding: utf-8 -*-
"""
FastAPI 애플리케이션 메인 엔트리포인트
REST API 엔드포인트 정의 및 백그라운드 자동투자 엔진 구동을 담당합니다.
"""

import asyncio
from fastapi import FastAPI, Depends, HTTPException, Query, Body
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import uvicorn
import logging

import config
from database import init_db, get_db, SplitConfig, SplitState, OrderLog, EmergencyLog
from kiwoom import KiwoomClient, KiwoomMockClient
from engine import TriggerEngine
from engine.trigger import log_system_event, system_log_queue

# 로깅 기본 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("MainServer")

app = FastAPI(title="7-Split AutoTrading System", version="0.1")

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 글로벌 인스턴스 보관용 딕셔너리
app_state = {}


@app.on_event("startup")
async def startup_event():
    """
    서버 시작 시 데이터베이스를 초기화하고, 설정에 맞는 키움 클라이언트와 자동투자 엔진을 실행합니다.
    """
    # 1. DB 테이블 초기화
    init_db()
    log_system_event("데이터베이스가 초기화되었습니다.", "INFO")

    # 2. 클라이언트 인스턴스화 (시뮬레이션 모드 vs 실제 키움 API)
    if config.SIMULATION_MODE:
        kiwoom_client = KiwoomMockClient(
            account_no=config.KIWOOM_ACCOUNT_NO,
            is_mock=True
        )
        log_system_event("[MODE] 로컬 시뮬레이션 모드로 작동을 개시합니다.", "WARNING")
    else:
        kiwoom_client = KiwoomClient(
            app_key=config.KIWOOM_APP_KEY,
            secret_key=config.KIWOOM_SECRET_KEY,
            account_no=config.KIWOOM_ACCOUNT_NO,
            is_mock=config.IS_MOCK_SERVER
        )
        log_system_event("[MODE] 실제 키움증권 API 연동 모드로 작동을 개시합니다.", "WARNING")

    # 3. 인증 실행
    auth_ok = await kiwoom_client.authenticate()
    if not auth_ok:
        log_system_event("키움 API 인증에 실패했습니다. 키 설정을 확인하세요.", "ERROR")

    # 4. 트리거 엔진 초기화 및 구동
    engine = TriggerEngine(kiwoom_client)
    await engine.start()

    # 인스턴스 전역 보관
    app_state["kiwoom"] = kiwoom_client
    app_state["engine"] = engine
    log_system_event("세븐스플릿 자동투자 시스템 기동 완료.", "INFO")


@app.on_event("shutdown")
async def shutdown_event():
    """
    서버 종료 시 자동투자 엔진과 연결 세션들을 안전하게 닫습니다.
    """
    engine = app_state.get("engine")
    if engine:
        await engine.stop()
    log_system_event("세븐스플릿 자동투자 시스템을 중지합니다.", "INFO")


# =====================================================================
# 화면 렌더링 라우터
# =====================================================================

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """
    대시보드 메인 HTML 페이지 반환
    """
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f:
            content = f.read()
        return HTMLResponse(content=content)
    except FileNotFoundError:
        return HTMLResponse(
            content="<h3>index.html 파일을 찾을 수 없습니다. templates/index.html 경로를 확인하세요.</h3>",
            status_code=404
        )


# =====================================================================
# REST API 라우터
# =====================================================================

@app.get("/api/status")
async def get_system_status():
    """
    현재 시스템 모드와 계좌 정보를 반환합니다.
    """
    kiwoom = app_state.get("kiwoom")
    return {
        "simulation_mode": config.SIMULATION_MODE,
        "account_no": config.KIWOOM_ACCOUNT_NO,
        "is_mock_server": config.IS_MOCK_SERVER if not config.SIMULATION_MODE else True,
        "connected": kiwoom is not None
    }


@app.get("/api/balance")
async def get_balance():
    """
    현재 예수금, 총자산 및 보유주식 현황을 키움 API(혹은 Mock)를 통해 조회합니다.
    """
    kiwoom = app_state.get("kiwoom")
    if not kiwoom:
        raise HTTPException(status_code=503, detail="API 클라이언트가 준비되지 않았습니다.")
    
    balance_res = await kiwoom.get_balance()
    return balance_res


@app.get("/api/configs")
async def get_configs(db: Session = Depends(get_db)):
    """
    등록된 세븐스플릿 종목 설정 및 각 스플릿 단계의 실시간 상태를 함께 조회합니다.
    """
    configs = db.query(SplitConfig).all()
    kiwoom = app_state.get("kiwoom")
    
    result = []
    for cfg in configs:
        # 각 종목별 가상 현재가 가져오기
        current_price = 0.0
        if kiwoom:
            current_price = await kiwoom.get_stock_price(cfg.stock_code)
            # 만어 현재가가 0이면 DB에 기록된 HOLD 단계들의 평단 중 하나로 초기 보완
            if current_price == 0.0:
                holding_state = db.query(SplitState).filter(
                    SplitState.config_id == cfg.id,
                    SplitState.status == "HOLD"
                ).first()
                if holding_state:
                    current_price = holding_state.buy_price

        states = db.query(SplitState).filter(SplitState.config_id == cfg.id).order_by(SplitState.step).all()
        
        # 종목별 단계 요약 및 평가금액 산출
        holdings_qty = 0.0
        total_buy_amt = 0.0
        active_step_count = 0
        
        state_list = []
        for s in states:
            is_hold = s.status == "HOLD"
            if is_hold:
                holdings_qty += s.quantity
                total_buy_amt += s.quantity * s.buy_price
                active_step_count += 1
            
            # 개별 단계별 평가손익 계산
            step_eval_profit = 0.0
            step_profit_rate = 0.0
            if is_hold and current_price > 0:
                step_eval_profit = (current_price - s.buy_price) * s.quantity
                step_profit_rate = ((current_price - s.buy_price) / s.buy_price) * 100

            state_list.append({
                "step": s.step,
                "status": s.status,
                "buy_price": s.buy_price,
                "quantity": s.quantity,
                "buy_time": s.buy_time.strftime("%Y-%m-%d %H:%M:%S") if s.buy_time else None,
                "eval_profit": round(step_eval_profit, 2),
                "profit_rate": round(step_profit_rate, 2)
            })

        # 종목별 합산 손익 계산
        avg_price = total_buy_amt / holdings_qty if holdings_qty > 0 else 0.0
        eval_amt = holdings_qty * current_price
        eval_profit = eval_amt - total_buy_amt
        profit_rate = (eval_profit / total_buy_amt * 100) if total_buy_amt > 0 else 0.0

        # 시뮬레이션 전용 시나리오 방향 조회
        scenario = "RANDOM"
        if config.SIMULATION_MODE and hasattr(kiwoom, "scenarios"):
            scenario = kiwoom.scenarios.get(cfg.stock_code, "RANDOM")

        result.append({
            "id": cfg.id,
            "stock_code": cfg.stock_code,
            "stock_name": cfg.stock_name,
            "total_steps": cfg.total_steps,
            "first_entry_amount": cfg.first_entry_amount,
            "drop_trigger_pct": cfg.drop_trigger_pct,
            "target_profit_pct": cfg.target_profit_pct,
            "reinvest_enabled": cfg.reinvest_enabled,
            "is_active": cfg.is_active,
            "current_price": current_price,
            "holdings_qty": holdings_qty,
            "avg_price": round(avg_price, 2),
            "eval_profit": round(eval_profit, 2),
            "profit_rate": round(profit_rate, 2),
            "active_step_count": active_step_count,
            "scenario": scenario,
            "steps": state_list
        })
    return result


@app.post("/api/configs")
async def add_config(
    stock_code: str = Body(..., embed=True),
    stock_name: str = Body(..., embed=True),
    total_steps: int = Body(7, embed=True),
    first_entry_amount: float = Body(..., embed=True),
    drop_trigger_pct: float = Body(2.0, embed=True),
    target_profit_pct: float = Body(3.0, embed=True),
    reinvest_enabled: bool = Body(True, embed=True),
    db: Session = Depends(get_db)
):
    """
    새로운 종목 분할 투자 설정을 등록합니다.
    """
    # 6자리 포맷 맞춤 (앞에 0 채우기)
    stock_code = stock_code.strip().zfill(6)
    stock_name = stock_name.strip()

    # 이미 동일 종목 코드가 있는지 확인
    exists = db.query(SplitConfig).filter(SplitConfig.stock_code == stock_code).first()
    if exists:
        raise HTTPException(status_code=400, detail="이미 등록된 종목 코드입니다.")

    try:
        new_cfg = SplitConfig(
            stock_code=stock_code,
            stock_name=stock_name,
            total_steps=total_steps,
            first_entry_amount=first_entry_amount,
            drop_trigger_pct=drop_trigger_pct,
            target_profit_pct=target_profit_pct,
            reinvest_enabled=reinvest_enabled,
            is_active=True
        )
        db.add(new_cfg)
        db.flush()  # ID 확보

        # 종목 등록 시 해당 단계(1~N) 상태도 WAIT로 생성
        for i in range(1, total_steps + 1):
            state = SplitState(
                config_id=new_cfg.id,
                step=i,
                status="WAIT",
                buy_price=0.0,
                quantity=0.0
            )
            db.add(state)
        
        db.commit()
        log_system_event(f"새 종목 설정 등록: {stock_name}({stock_code}) {total_steps}단계", "INFO")
        
        # 엔진 감시 목록 갱신
        engine = app_state.get("engine")
        if engine:
            await engine.reload_configs()

        return {"success": True, "message": "종목 설정이 성공적으로 등록되었습니다."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/configs/{config_id}")
async def delete_config(config_id: int, db: Session = Depends(get_db)):
    """
    종목 분할 설정을 삭제합니다. (상태 데이터도 함께 cascade 삭제됨)
    """
    cfg = db.query(SplitConfig).filter(SplitConfig.id == config_id).first()
    if not cfg:
        raise HTTPException(status_code=404, detail="설정을 찾을 수 없습니다.")

    try:
        name = cfg.stock_name
        code = cfg.stock_code
        db.delete(cfg)
        db.commit()
        log_system_event(f"종목 설정 삭제 완료: {name}({code})", "INFO")
        
        # 엔진 감시 목록 갱신
        engine = app_state.get("engine")
        if engine:
            await engine.reload_configs()

        return {"success": True, "message": "성공적으로 삭제되었습니다."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/configs/{config_id}/toggle")
async def toggle_active(config_id: int, db: Session = Depends(get_db)):
    """
    특정 종목의 자동 감시(자동 매매)를 일시 중지하거나 재개합니다.
    """
    cfg = db.query(SplitConfig).filter(SplitConfig.id == config_id).first()
    if not cfg:
        raise HTTPException(status_code=404, detail="설정을 찾을 수 없습니다.")

    try:
        cfg.is_active = not cfg.is_active
        db.commit()
        status_str = "재개" if cfg.is_active else "일시 정지"
        log_system_event(f"종목 {cfg.stock_name}({cfg.stock_code})의 자동매매를 {status_str}합니다.", "INFO")
        
        # 엔진 감시 목록 갱신
        engine = app_state.get("engine")
        if engine:
            await engine.reload_configs()

        return {"success": True, "is_active": cfg.is_active}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/emergency")
async def emergency_clear_all():
    """
    전 종목 긴급 일괄 시장가 청산을 요청합니다.
    """
    engine = app_state.get("engine")
    if not engine:
        raise HTTPException(status_code=503, detail="엔진이 구동 상태가 아닙니다.")
    
    success = await engine.execute_emergency_liquidation()
    if success:
        return {"success": True, "message": "전 종목 긴급 청산 주문이 정상적으로 실행되었습니다."}
    else:
        return {"success": False, "message": "일부 또는 전체 종목의 긴급 청산 과정에 오류가 발생했습니다."}


@app.post("/api/emergency/{config_id}")
async def emergency_clear_one(config_id: int):
    """
    특정 종목을 즉시 긴급 청산(전량 매도)합니다.
    """
    engine = app_state.get("engine")
    if not engine:
        raise HTTPException(status_code=533, detail="엔진이 구동 상태가 아닙니다.")
    
    success = await engine.execute_emergency_liquidation(config_id=config_id)
    if success:
        return {"success": True, "message": "해당 종목 긴급 청산 주문이 정상 실행되었습니다."}
    else:
        return {"success": False, "message": "해당 종목 긴급 청산 도중 오류가 발생했습니다."}


# =====================================================================
# 시뮬레이션 전용 조작 API
# =====================================================================

@app.post("/api/mock/scenario")
async def set_mock_scenario(
    stock_code: str = Body(..., embed=True),
    scenario: str = Body(..., embed=True)  # UP, DOWN, SIDE, RANDOM
):
    """
    시뮬레이션 모드일 때 가상 주가의 변동 시나리오를 설정합니다. (UP/DOWN/SIDE/RANDOM)
    """
    if not config.SIMULATION_MODE:
        raise HTTPException(status_code=400, detail="시뮬레이션 모드가 아닐 때는 사용할 수 없습니다.")

    kiwoom = app_state.get("kiwoom")
    if kiwoom and hasattr(kiwoom, "set_price_scenario"):
        kiwoom.set_price_scenario(stock_code, scenario.upper())
        return {"success": True, "scenario": scenario}
    else:
        raise HTTPException(status_code=503, detail="Mock 클라이언트가 제대로 초기화되지 않았습니다.")


# =====================================================================
# 실시간 시스템 로그 스트리밍 (SSE)
# =====================================================================

@app.get("/api/logs")
async def stream_logs():
    """
    Server-Sent Events(SSE) 방식으로 백엔드 시스템 로그를 실시간 브라우저로 전송합니다.
    """
    async def log_generator():
        # 첫 접속 시 가벼운 안내 메시지 전송
        yield f"data: [SYSTEM] 실시간 로그 스트림에 접속되었습니다. 모드: {'시뮬레이션' if config.SIMULATION_MODE else '실전'}\n\n"
        while True:
            try:
                # 큐에 새 로그 메시지가 들어올 때까지 대기
                log_msg = await system_log_queue.get()
                yield f"data: {log_msg}\n\n"
                system_log_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                yield f"data: [SYSTEM ERROR] 로그 스트리밍 예외 발생: {str(e)}\n\n"
                await asyncio.sleep(2)

    return StreamingResponse(log_generator(), media_type="text/event-stream")


if __name__ == "__main__":
    # 포트 설정을 config 파일에 정의된 포트로 셋업
    uvicorn.run("main:app", host="0.0.0.0", port=config.PORT, reload=False)

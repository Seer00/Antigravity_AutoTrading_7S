# -*- coding: utf-8 -*-
"""
키움증권 REST 및 WebSocket API 비동기 클라이언트
실제 키움증권 오픈 API 서비스와 연동하여 토큰 발급, 주문, 계좌 잔고 조회, 실시간 체결가 수신을 처리합니다.
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
import websockets

logger = logging.getLogger("KiwoomClient")

class KiwoomClient:
    """
    키움증권 REST API 및 WebSocket 실시간 시세 연동 클라이언트
    """
    def __init__(self, app_key: str = None, secret_key: str = None, account_no: str = None, is_mock: bool = True):
        self.app_key = app_key or os.getenv("KIWOOM_APP_KEY", "")
        self.secret_key = secret_key or os.getenv("KIWOOM_SECRET_KEY", "")
        self.account_no = account_no or os.getenv("KIWOOM_ACCOUNT_NO", "")
        self.is_mock = is_mock
        
        # 환경별 도메인 설정
        if self.is_mock:
            self.base_url = "https://mockapi.kiwoom.com"
            self.ws_url = "wss://mockapi.kiwoom.com:10000"
        else:
            self.base_url = "https://api.kiwoom.com"
            self.ws_url = "wss://api.kiwoom.com:10000"

        self.access_token = ""
        self.token_expire_time = None
        self._session = None
        self.ws_client = None
        self.ws_task = None
        self.price_callbacks = []  # 실시간 시세 수신 시 호출할 콜백 리스트

    async def get_session(self):
        """
        비동기 HTTP 세션을 반환합니다.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        """
        세션 및 웹소켓 연결을 해제합니다.
        """
        if self._session and not self._session.closed:
            await self._session.close()
        if self.ws_client:
            await self.ws_client.close()
        if self.ws_task:
            self.ws_task.cancel()

    async def authenticate(self) -> bool:
        """
        OAuth2 접근 토큰을 발급받거나 갱신합니다.
        """
        if not self.app_key or not self.secret_key:
            logger.error("키움 API 앱키 또는 시크릿키가 설정되지 않았습니다.")
            return False

        # 기존 토큰의 유효 시간 검증 (5분 이상 남았는지 확인)
        if self.access_token and self.token_expire_time:
            if datetime.now() < self.token_expire_time - timedelta(minutes=5):
                return True

        url = f"{self.base_url}/oauth2/token"
        headers = {"Content-Type": "application/json;charset=UTF-8"}
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.secret_key
        }

        try:
            session = await self.get_session()
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    res_json = await response.json()
                    self.access_token = res_json.get("access_token", "")
                    expires_in = int(res_json.get("expires_in", 3600))
                    self.token_expire_time = datetime.now() + timedelta(seconds=expires_in)
                    logger.info("접근 토큰 발급 성공. 유효시간: %s초", expires_in)
                    return True
                else:
                    err_text = await response.text()
                    logger.error("접근 토큰 발급 실패. HTTP 상태코드: %s, 응답: %s", response.status, err_text)
                    return False
        except Exception as e:
            logger.error("접근 토큰 발급 중 예외 발생: %s", str(e))
            return False

    async def _request_tr(self, tr_code: str, endpoint: str, payload: dict) -> dict:
        """
        키움 REST TR 요청을 전송하는 공통 메서드
        """
        await self.authenticate()
        
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "Authorization": f"Bearer {self.access_token}",
            "api-id": tr_code,
            "cont-yn": "N",
            "next-key": ""
        }

        try:
            session = await self.get_session()
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    err_text = await response.text()
                    logger.error("TR 요청 실패 (%s). HTTP 상태: %s, 응답: %s", tr_code, response.status, err_text)
                    return {"error": f"HTTP {response.status}", "message": err_text}
        except Exception as e:
            logger.error("TR 요청 중 예외 발생 (%s): %s", tr_code, str(e))
            return {"error": "EXCEPTION", "message": str(e)}

    async def get_balance(self) -> dict:
        """
        계좌 평가 잔고 및 보유 종목 리스트를 조회합니다. (TR: kt00018)
        """
        endpoint = "/api/v1/dostk/acnt"
        payload = {
            "acct_no": self.account_no,
            "pwd": "", # 모의투자는 빈 값 허용, 실전은 필요 시 환경변수로 대체
            "nrec": "0" # 0: 전체 조회
        }
        
        # 키움 REST API의 실제 응답 형식에 맞춰 데이터를 래핑하여 파싱하기 쉽도록 가공합니다.
        res = await self._request_tr("kt00018", endpoint, payload)
        if "error" in res:
            return {"success": False, "message": res["message"]}
            
        return {
            "success": True,
            "raw_data": res,
            "deposit": float(res.get("dnca_tot_amt", 0)),      # 예수금 총액 (또는 d2예수금)
            "total_asset": float(res.get("tot_evlu_amt", 0)),  # 총 평가 금액 (예수금 + 주식평가액)
            # 보유 종목 리스트 가공
            "holdings": [
                {
                    "stock_code": item.get("stk_cd", ""),      # 종목코드
                    "stock_name": item.get("stk_nm", ""),      # 종목명
                    "quantity": float(item.get("hld_qty", 0)),  # 보유 수량
                    "buy_price": float(item.get("pchs_avg_price", 0)), # 매수 평단가
                    "current_price": float(item.get("now_price", 0)), # 현재가
                    "eval_profit": float(item.get("evlu_pl_amt", 0)), # 평가 손익
                    "profit_rate": float(item.get("evlu_erng_rt", 0)) # 평가 수익률 (%)
                }
                for item in res.get("out_lst", []) if int(item.get("hld_qty", 0)) > 0
            ]
        }

    async def get_stock_price(self, stock_code: str) -> float:
        """
        특정 종목의 현재가를 단일 조회합니다. (TR: ka10001)
        """
        endpoint = "/api/v1/dostk/stock"
        payload = {
            "stk_cd": stock_code
        }
        res = await self._request_tr("ka10001", endpoint, payload)
        if "error" in res:
            logger.error("현재가 조회 실패 (%s): %s", stock_code, res["message"])
            return 0.0
        
        # 현재가는 부호 제거 후 절댓값으로 처리
        current_price = abs(float(res.get("now_price", 0)))
        return current_price

    async def place_order(self, stock_code: str, order_type: str, price: float, quantity: int) -> dict:
        """
        주식을 주문합니다.
        order_type: BUY (매수) -> TR kt10000, SELL (매도) -> TR kt10001
        price: 0 (시장가 주문 시 0 또는 빈값 전달), 지정가인 경우 해당 단가
        """
        tr_code = "kt10000" if order_type == "BUY" else "kt10001"
        endpoint = "/api/v1/dostk/ordr"
        
        # trde_tp (거래구분): 3 -> 시장가, 0 -> 지정가
        trde_tp = "3" if price == 0 else "0"
        
        payload = {
            "acct_no": self.account_no,
            "stk_cd": stock_code,
            "ord_qty": str(quantity),
            "ord_uv": str(price) if price > 0 else "",
            "trde_tp": trde_tp
        }

        res = await self._request_tr(tr_code, endpoint, payload)
        if "error" in res:
            return {"success": False, "message": res["message"]}

        return {
            "success": True,
            "order_no": res.get("ord_no", ""), # 주문 번호
            "raw_data": res
        }

    def register_price_callback(self, callback):
        """
        실시간 시세 수신 시 트리거할 콜백 함수를 등록합니다.
        callback의 인자는 (stock_code, current_price) 형태여야 합니다.
        """
        self.price_callbacks.append(callback)

    async def start_websocket(self, stock_codes: list[str]):
        """
        WebSocket에 접속하여 지정된 종목들의 실시간 시세(체결가) 수신을 시작합니다.
        """
        if not stock_codes:
            logger.warning("WebSocket 감시할 종목코드가 없습니다.")
            return

        await self.authenticate()
        
        async def listen_loop():
            uri = f"{self.ws_url}/ws" # 상세 WebSocket 엔드포인트는 공식 문서 기준
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }
            
            while True:
                try:
                    logger.info("WebSocket 서버 연결 시도: %s", self.ws_url)
                    async with websockets.connect(uri, extra_headers=headers) as ws:
                        self.ws_client = ws
                        logger.info("WebSocket 서버 연결 성공")
                        
                        # 구독 패킷 전송 (종목 실시간 등록)
                        subscribe_packet = {
                            "header": {
                                "appkey": self.app_key,
                                "token": self.access_token,
                                "custtype": "P" if not self.is_mock else "T",
                                "tr_type": "1" # 1: 등록, 2: 해제
                            },
                            "body": {
                                "input": {
                                    "tr_id": "H0STCNT0", # 실시간 체결가 TR ID (키움증권 REST 명세 기준)
                                    "tr_key": ",".join(stock_codes)
                                }
                            }
                        }
                        await ws.send(json.dumps(subscribe_packet))
                        
                        while True:
                            message = await ws.recv()
                            try:
                                data = json.loads(message)
                                # 실시간 데이터 구조 파싱 (공식 가이드 준수)
                                # 실시간 데이터에 체결가가 들어오면 콜백 호출
                                # 예시: {"stk_cd": "005930", "now_price": "71500"}
                                body = data.get("body", {})
                                stock_code = body.get("stk_cd")
                                now_price_str = body.get("now_price")
                                
                                if stock_code and now_price_str:
                                    current_price = abs(float(now_price_str))
                                    for cb in self.price_callbacks:
                                        if asyncio.iscoroutinefunction(cb):
                                            await cb(stock_code, current_price)
                                        else:
                                            cb(stock_code, current_price)
                            except json.JSONDecodeError:
                                # 바이너리 혹은 파이프 구분 텍스트 데이터의 경우 문자열 파싱
                                if isinstance(message, str) and "|" in message:
                                    # 키움 웹소켓 포맷: 리시브 타입에 따른 데이터 스플릿
                                    parts = message.split("|")
                                    # 실시간 수신 메시지 파싱 처리
                                    pass
                            
                except asyncio.CancelledError:
                    logger.info("WebSocket 리스너 태스크 취소")
                    break
                except Exception as e:
                    logger.error("WebSocket 통신 오류, 5초 후 재연결 시도: %s", str(e))
                    await asyncio.sleep(5)

        self.ws_task = asyncio.create_task(listen_loop())

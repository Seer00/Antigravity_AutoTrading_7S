# -*- coding: utf-8 -*-
"""
환경 설정 로더
.env 파일 또는 시스템 환경 변수로부터 설정을 읽어옵니다.
"""

import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

SIMULATION_MODE = os.getenv("SIMULATION_MODE", "True").lower() == "true"
KIWOOM_APP_KEY = os.getenv("KIWOOM_APP_KEY", "")
KIWOOM_SECRET_KEY = os.getenv("KIWOOM_SECRET_KEY", "")
KIWOOM_ACCOUNT_NO = os.getenv("KIWOOM_ACCOUNT_NO", "")
IS_MOCK_SERVER = os.getenv("IS_MOCK_SERVER", "True").lower() == "true"
PORT = int(os.getenv("PORT", "8000"))

print(f"[설정 로드 완료] 시뮬레이션 모드: {SIMULATION_MODE}, 계좌번호: {KIWOOM_ACCOUNT_NO}")

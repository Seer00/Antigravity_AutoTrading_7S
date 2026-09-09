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

# 전역 설정 로드 함수
def reload_config():
    global SIMULATION_MODE, KIWOOM_APP_KEY, KIWOOM_SECRET_KEY, KIWOOM_ACCOUNT_NO, IS_MOCK_SERVER, PORT
    load_dotenv(override=True)
    SIMULATION_MODE = os.getenv("SIMULATION_MODE", "True").lower() == "true"
    KIWOOM_APP_KEY = os.getenv("KIWOOM_APP_KEY", "")
    KIWOOM_SECRET_KEY = os.getenv("KIWOOM_SECRET_KEY", "")
    KIWOOM_ACCOUNT_NO = os.getenv("KIWOOM_ACCOUNT_NO", "")
    IS_MOCK_SERVER = os.getenv("IS_MOCK_SERVER", "True").lower() == "true"
    PORT = int(os.getenv("PORT", "8000"))

def save_config_to_env(new_settings: dict):
    """
    환경 변수 딕셔너리를 입력받아 .env 파일에 저장하고 파이썬 프로세스 전역 변수를 갱신합니다.
    """
    env_path = ".env"
    env_dict = {}
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_dict[k.strip()] = v.strip()
    
    # 설정값 갱신
    for k, v in new_settings.items():
        env_dict[k] = str(v)
        os.environ[k] = str(v)

    # .env 파일 쓰기
    with open(env_path, "w", encoding="utf-8") as f:
        for k, v in env_dict.items():
            f.write(f"{k}={v}\n")
            
    reload_config()
    print(f"[설정 저장 완료] 시뮬레이션 모드: {SIMULATION_MODE}, 계좌번호: {KIWOOM_ACCOUNT_NO}")

print(f"[설정 로드 완료] 시뮬레이션 모드: {SIMULATION_MODE}, 계좌번호: {KIWOOM_ACCOUNT_NO}")

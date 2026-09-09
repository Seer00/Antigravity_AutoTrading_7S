# 📈 AutoTrading_7S — 세븐스플릿(7-Split) 자동투자 프로그램

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688)
![License](https://img.shields.io/badge/license-MIT-green)

키움증권 REST API를 활용하여 **세븐스플릿(7-Split) 주식 분할 매수·매도 전략**을 자동화한 경량 주식 자동매매 프로그램입니다.  
부가기능을 배제하고 **핵심 분할 매수 및 분할 매도 로직**에 집중하여 안정적인 평단가 관리 및 이익 실현을 도옵니다.

---

## ✨ 주요 기능 (Key Features)

- **N단계 분할 매수 (최대 7단계)**
  - 사용자가 설정한 종목별 하락 트리거(%)에 맞춰 단계별(넘버1~N) 자동 분할 매수 실행.
- **단계별 독립 분할 매도**
  - 각 단계별 매수단가 및 수량을 독립 관리하며, 목표 수익률(%) 도달 시 해당 단계만 자동 매도.
- **키움증권 REST API 연동 & 모의투자 모드 지원**
  - 실계좌 거래 뿐만 아니라 `MOCK_MODE`를 통한 사전 안전 검증 모환 지원.
- **FastAPI 웹 대시보드**
  - 보유 종목 한눈에 보기, 단계별 진행 상태 조회, 매매 설정 등록 및 수정.
- **수동 긴급 청산 (안전장치)**
  - 시세 급락 또는 이상 발생 시, 사용자가 수동으로 즉시 전량 매도할 수 있는 긴급 청산 기능.
- **매매 이력 및 거래 상태 저장**
  - SQLite 및 SQLAlchemy ORM을 통한 실시간 매매 로그 및 주문 체결 내역 기록.

---

## 🛠 기술 스택 (Tech Stack)

- **Language**: Python 3.10+
- **Framework**: FastAPI, Uvicorn
- **Database**: SQLite, SQLAlchemy ORM
- **HTTP/WebSockets**: `aiohttp`, `websockets`
- **UI/Visuals**: HTML5, Vanilla CSS, JavaScript, `rich`, `koreanize-matplotlib`

---

## 📂 디렉토리 구조 (Directory Structure)

```text
AutoTrading_7S/
├── 7s_plan_claude.md   # 세부 SW 개발계획서 및 유스케이스 정의
├── config.py           # 시스템 설정 및 환경 변수 로드
├── database.py         # SQLAlchemy DB 모델 및 연결 관리
├── main.py             # FastAPI 애플리케이션 엔트리포인트
├── requirements.txt    # 프로젝트 의존성 라이브러리 목록
├── kiwoom/             # 키움증권 REST API 클라이언트 모듈
│   ├── client.py       # 실전 API 클라이언트
│   └── mock_client.py  # 모의투자 및 테스트용 클라이언트
├── engine/             # 자동매매 핵심 매매 트리거 엔진
│   └── trigger.py      # 실시간 시세 감시 및 자동 주문 실행 로직
└── templates/          # 웹 대시보드 HTML 템플릿
    └── index.html
```

---

## 🚀 시작하기 (Quick Start)

### 1. 저장소 클론 (Clone Repository)

```bash
git clone https://github.com/Seer00/Antigravity_AutoTrading_7S.git
cd Antigravity_AutoTrading_7S
```

### 2. 가상환경 구축 및 패키지 설치 (Virtual Environment & Dependencies)

`uv` 사용 시:
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

기본 `pip` 사용 시:
```bash
python -m venv .venv
source .venv/bin/activate  # macOS / Linux
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

### 3. 환경 변수 설정 (`.env`)

프로젝트 루트 디렉토리에 `.env` 파일을 생성하고 키움증권 API 및 서비스 설정을 입력합니다.

```env
KIWOOM_APP_KEY=your_app_key_here
KIWOOM_APP_SECRET=your_app_secret_here
KIWOOM_ACCOUNT_NO=your_account_number
MOCK_MODE=True
```

### 4. 프로그램 실행 (Run Application)

```bash
python main.py
```
실행 후 웹 브라우저에서 `http://localhost:8000`에 접속하여 대시보드를 확인할 수 있습니다.

---

## ⚠️ 면책 조항 (Disclaimer)

- 본 프로그램은 주식 투자 참고용 및 자동화 도구이며, **투자의 최종 책임은 사용자 본인에게 있습니다.**
- 실계좌 매매 전 반드시 **모의투자 모드(`MOCK_MODE=True`)에서 충분한 검증**을 거친 후 사용하시기 바랍니다.

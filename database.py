# -*- coding: utf-8 -*-
"""
데이터베이스 모듈
SQLite 데이터베이스 연결을 설정하고, 세븐스플릿 프로그램에 필요한 테이블 스키마를 SQLAlchemy로 정의합니다.
한국어 주석 규칙을 준수합니다.
"""

import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# DB 파일명 정의
DB_FILE = "autotrading_7s.db"
DATABASE_URL = f"sqlite:///{DB_FILE}"

# SQLAlchemy Base 모델 선언
Base = declarative_base()

class SplitConfig(Base):
    """
    세븐스플릿 종목별 설정 정보 테이블
    """
    __tablename__ = "split_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), unique=True, nullable=False, index=True) # 종목코드 (예: 005930)
    stock_name = Column(String(50), nullable=False)                         # 종목명 (예: 삼성전자)
    total_steps = Column(Integer, default=7, nullable=False)               # 최대 단계 수 (기본 7스플릿)
    first_entry_amount = Column(Float, nullable=False)                     # 1단계(최초) 매수 금액 (원 단위)
    drop_trigger_pct = Column(Float, nullable=False, default=2.0)          # 추가 매수 하락 트리거 (%)
    target_profit_pct = Column(Float, nullable=False, default=3.0)         # 목표 수익률 (%)
    reinvest_enabled = Column(Boolean, default=True, nullable=False)       # 매도 완료 시 해당 단계 재투자(대기 상태 복귀) 여부
    is_active = Column(Boolean, default=True, nullable=False)               # 자동 감시 활성화 여부
    created_at = Column(DateTime, default=datetime.now)

    # 1:N 관계 설정 (상태 테이블 및 주문 이력 테이블)
    states = relationship("SplitState", back_populates="config", cascade="all, delete-orphan")
    orders = relationship("OrderLog", back_populates="config", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<SplitConfig {self.stock_name}({self.stock_code}) steps={self.total_steps}>"


class SplitState(Base):
    """
    각 종목별 스플릿 단계(1~N)의 현재 상태를 관리하는 테이블
    """
    __tablename__ = "split_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_id = Column(Integer, ForeignKey("split_config.id", ondelete="CASCADE"), nullable=False)
    step = Column(Integer, nullable=False)                                 # 단계 번호 (1 ~ N)
    status = Column(String(20), default="WAIT", nullable=False)            # 상태: WAIT (대기), HOLD (보유), SOLD (매도완료)
    buy_price = Column(Float, default=0.0, nullable=False)                 # 이 단계의 매수 단가
    quantity = Column(Float, default=0.0, nullable=False)                  # 이 단계의 보유 수량
    buy_time = Column(DateTime, nullable=True)                              # 매수 체결 일시

    config = relationship("SplitConfig", back_populates="states")

    def __repr__(self):
        return f"<SplitState config_id={self.config_id} step={self.step} status={self.status} price={self.buy_price}>"


class OrderLog(Base):
    """
    자동매매로 발생한 주문 및 체결 내역 로그 테이블
    """
    __tablename__ = "order_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_id = Column(Integer, ForeignKey("split_config.id", ondelete="CASCADE"), nullable=True)
    step = Column(Integer, nullable=True)                                  # 관련 스플릿 단계
    order_type = Column(String(10), nullable=False)                        # 주문 구분: BUY (매수), SELL (매도)
    order_price = Column(Float, nullable=False)                            # 주문 단가
    order_quantity = Column(Float, nullable=False)                         # 주문 수량
    executed_price = Column(Float, default=0.0)                            # 체결 단가
    executed_quantity = Column(Float, default=0.0)                         # 체결 수량
    status = Column(String(20), default="REQUESTED", nullable=False)       # 상태: REQUESTED (요청), FILLED (체결완료), REJECTED (거부), CANCELLED (취소)
    order_time = Column(DateTime, default=datetime.now)

    config = relationship("SplitConfig", back_populates="orders")

    def __repr__(self):
        return f"<OrderLog type={self.order_type} step={self.step} status={self.status}>"


class EmergencyLog(Base):
    """
    사용자가 수동으로 실행한 긴급 청산(일괄 시장가 매도) 이력 테이블
    """
    __tablename__ = "emergency_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False, index=True)            # 청산한 종목 코드 (전체 청산 시 'ALL')
    stock_name = Column(String(50), nullable=False)                        # 청산한 종목 명
    quantity = Column(Float, nullable=False)                               # 청산 시 매도된 총 수량
    reason = Column(String(255), nullable=True)                            # 긴급 청산 사유 (사용자 입력)
    executed_time = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<EmergencyLog stock={self.stock_code} qty={self.quantity} time={self.executed_time}>"


# 엔진 및 세션 팩토리 초기화
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """
    데이터베이스 테이블들을 생성합니다.
    """
    Base.metadata.create_all(bind=engine)

def get_db():
    """
    FastAPI 의존성 주입을 위한 DB 세션 제너레이터
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

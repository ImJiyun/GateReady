"""
실시간 데이터 수집 (5분마다)
"""
import sys

from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

src_path = str(Path(__file__).resolve().parent.parent)
if src_path not in sys.path:
    sys.path.append(src_path)
    
from clients.session import build_session
from config import (
    FLIGHT_API_URL, 
    HEADERS, 
    DEFAULT_API_TIMEOUT
)
from collectors.bronze import transform, upload_to_bq  

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

def get_time_window(hours_before=3, hours_after=3):
    """현재 시각 ±3시간 윈도우"""
    now = datetime.now(KST)
    
    start_dt = now - timedelta(hours=hours_before)
    end_dt = now + timedelta(hours=hours_after)
    
    start_time = start_dt.strftime("%H%M")
    end_time = end_dt.strftime("%H%M")
    
    # 자정 넘어갈 경우 처리
    if start_dt.date() < now.date():
        start_time = "0000"
    if end_dt.date() > now.date():
        end_time = "2359"
    
    return start_time, end_time


def fetch_current_flights(session: requests.Session) -> list[dict]:
    """현재 진행 중인 항공편 조회"""
    now = datetime.now(KST)
    ymd = now.strftime("%Y%m%d")
    start_time, end_time = get_time_window()
    
    payload = {
        "searchContext": "departure",
        "ymd": ymd,
        "startTime": start_time,  # 동적
        "endTime": end_time,      # 동적
        "loadType": "json",
        "depAirport": "RKSI",
    }
    
    logger.debug(f"Query: {ymd} {start_time}-{end_time}")
    
    res = session.post(
        FLIGHT_API_URL,
        json=payload,
        headers=HEADERS,
        timeout=DEFAULT_API_TIMEOUT,
    )
    
    res.raise_for_status()
    records = res.json().get("content", [])
    
    # 필터: 아직 안 떴거나 최근 1시간 이내
    filtered = []
    cutoff = now - timedelta(hours=1)
    
    for r in records:
        actual = r.get("actualFlightTime")
        
        if not actual:
            filtered.append(r)  # 아직 안 떴음
            continue
        
        # 떴는데 최근이면 포함
        try:
            actual_dt = datetime.strptime(
                f"{ymd} {actual}", 
                "%Y%m%d %H:%M"
            ).replace(tzinfo=KST)
            
            if actual_dt >= cutoff:
                filtered.append(r)
        except:
            filtered.append(r)
    
    logger.info(f"Active flights: {len(filtered)}/{len(records)}")
    return filtered


def collect_realtime():
    """실시간 수집 메인"""
    session = build_session()
    
    try:
        records = fetch_current_flights(session)
        
        if not records:
            logger.info("No active flights found at this time.")
            return
        
        ymd = datetime.now(KST).strftime("%Y%m%d")
        df = transform(records, ymd)
        upload_to_bq(df)
        
        logger.info(f"Collected and uploaded {len(df)} flights")
        
    except Exception:
        logger.exception("Failed to collect realtime flights")
        raise


if __name__ == "__main__":
    # 직접 실행 시 로거 기본 설정 적용
    logging.basicConfig(level=logging.INFO)
    collect_realtime()
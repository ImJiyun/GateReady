"""
실시간 데이터 수집 (10분마다)
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from zoneinfo import ZoneInfo
    
from src.clients.session import build_session
from src.config import (
    FLIGHT_API_URL,
    HEADERS,
    DEFAULT_API_TIMEOUT,
    REALTIME_COLLECTION_HOURS_BEFORE,
    REALTIME_COLLECTION_HOURS_AFTER
)
from src.collectors.bronze import transform, upload_to_bq

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

def get_time_windows(hours_before=3, hours_after=3):
    """
    현재 시각 기준 수집 윈도우 생성 (날짜 경계 처리 포함)
    통계적 지연 분석을 위해 예정 시각 기준 ±3시간 범위를 수집합니다.
    """
    now = datetime.now(KST)
    start_dt = now - timedelta(hours=hours_before)
    end_dt = now + timedelta(hours=hours_after)
    
    windows = []
    
    # 시작일과 종료일이 같은 경우
    if start_dt.date() == end_dt.date():
        windows.append((
            start_dt.strftime("%Y%m%d"),
            start_dt.strftime("%H%M"),
            end_dt.strftime("%H%M")
        ))
    # 날짜가 바뀌는 경우 (자정을 지날 때)
    else:
        # 오늘 치 (시작 시각 ~ 23:59)
        windows.append((
            start_dt.strftime("%Y%m%d"),
            start_dt.strftime("%H%M"),
            "2359"
        ))
        # 다음 날 치 (00:00 ~ 종료 시각)
        windows.append((
            end_dt.strftime("%Y%m%d"),
            "0000",
            end_dt.strftime("%H%M")
        ))
    
    return windows


def fetch_current_flights(session: requests.Session) -> list[tuple[dict, str]]:
    """
    현재 진행 중인 항공편 조회 (다중 윈도우 지원)
    Returns: list of (record_dict, ymd_string) 튜플
    """
    windows = get_time_windows(hours_before=REALTIME_COLLECTION_HOURS_BEFORE,
                               hours_after=REALTIME_COLLECTION_HOURS_AFTER)
    all_results = []
    
    for ymd, start_time, end_time in windows:
        payload = {
            "searchContext": "departure",
            "ymd": ymd,
            "startTime": start_time,
            "endTime": end_time,
            "loadType": "json",
            "depAirport": "RKSI",
        }
        
        logger.info(f"Fetching flights: {ymd} {start_time}-{end_time}")
        
        res = session.post(
            FLIGHT_API_URL,
            json=payload,
            headers=HEADERS,
            timeout=DEFAULT_API_TIMEOUT,
        )
        
        res.raise_for_status()
        records = res.json().get("content", [])
        
        # 각 레코드와 해당 날짜를 페어링
        for r in records:
            all_results.append((r, ymd))
            
    logger.info(f"Total flights fetched across {len(windows)} windows: {len(all_results)}")
    return all_results


def collect_realtime():
    """실시간 수집 메인"""
    session = build_session()
    
    try:
        # (record, ymd) 형태의 튜플 리스트 반환
        pairs = fetch_current_flights(session)
        
        if not pairs:
            logger.info("No active flights found at this time.")
            return
        
        # 날짜별로 그룹화하여 처리
        from collections import defaultdict
        grouped = defaultdict(list)
        for record, ymd in pairs:
            grouped[ymd].append(record)
            
        total_count = 0
        for ymd, records in grouped.items():
            df = transform(records, ymd)
            upload_to_bq(df)
            total_count += len(df)
            logger.info(f"[{ymd}] Uploaded {len(df)} flights to Bronze")
            
        logger.info(f"Done. Collected {total_count} flights to Bronze.")
        
    except Exception:
        logger.exception("Failed to collect realtime flights")
        raise


if __name__ == "__main__":
    # 직접 실행 시 로거 기본 설정 적용
    logging.basicConfig(level=logging.INFO)
    collect_realtime()
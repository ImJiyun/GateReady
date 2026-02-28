import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime, timedelta, UTC, timezone
from zoneinfo import ZoneInfo
from google.cloud import bigquery
import os, re, json
import time, random
from dotenv import load_dotenv
import sys
from pathlib import Path
import logging

# 현재 파일의 부모 디렉토리(src/)를 시스템 경로에 추가하여 
# 다른 모듈을 찾을 수 있게 합니다.
src_path = str(Path(__file__).resolve().parent.parent)
if src_path not in sys.path:
    sys.path.append(src_path)

from clients.session import build_session
from config import BQ_PROJECT_ID, BRONZE_FLIGHTS_TABLE_ID, FLIGHT_API_URL, HEADERS, DEFAULT_API_TIMEOUT

logger = logging.getLogger(__name__)

def date_range(start: str, end: str):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    while start_dt <= end_dt:
        yield start_dt.strftime("%Y%m%d")
        start_dt += timedelta(days=1)


def fetch_flights(session: requests.Session, ymd: str) -> list[dict]:
    payload = {
        "searchContext": "departure",
        "ymd": ymd,
        "startTime": "0000",
        "endTime": "2400",
        "loadType": "json",
        "depAirport": "RKSI",
    }


    res = session.post(
        FLIGHT_API_URL,
        json=payload,
        headers=HEADERS,
        timeout=DEFAULT_API_TIMEOUT,
    )

    # 4xx/5xx 최종 실패 처리
    res.raise_for_status()
    return res.json().get("content", [])

def transform(records: list[dict], ymd: str) -> pd.DataFrame:
    rows = []
    collected_at = datetime.now(UTC)

    for r in records:
        flight_iata = r.get("fpIata")

        row = {
            "flight_key": f"{ymd}_{flight_iata}",

            "airline_icao": r.get("alIcao"),
            "airline_kr": r.get("alKr"),
            "flight_iata": flight_iata,

            "arr_airport_iata": r.get("apIata"),
            "arr_airport_kr": r.get("apKr"),

            "status": r.get("status"),
            "status_remark": r.get("statusRemark"),
            "status_remark_code": r.get("statusRemarkCode"),

            "scheduled_time": r.get("schTime"),
            "expected_time": r.get("expectedFlightTime"),
            "actual_time": r.get("actualFlightTime"),

            "nature": r.get("nature"),
            "ymd": ymd,
            "collected_at": collected_at,
            "raw_json": json.dumps(r, ensure_ascii=False),
        }

        rows.append(row)

    return pd.DataFrame(rows)


def upload_to_bq(df: pd.DataFrame):
    client = bigquery.Client(project=BQ_PROJECT_ID)

    job = client.load_table_from_dataframe(
        df,
        BRONZE_FLIGHTS_TABLE_ID,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )
    job.result()


def collect_bronze_range(start_date: str, end_date: str):
    """지정한 날짜 범위의 항공편 데이터를 수집하여 BigQuery에 저장합니다."""
    session = build_session()
    
    # 세션 활성화를 위한 초기 요청
    try:
        session.get(FLIGHT_API_URL, timeout=DEFAULT_API_TIMEOUT)
    except Exception as e:
        logger.error(f"Failed to initialize session: {e}")
        return

    failed_days = []

    for ymd in date_range(start_date, end_date):
        try:
            records = fetch_flights(session, ymd) 

            if not records:
                logger.info(f"{ymd}: no data")
                time.sleep(random.uniform(0.8, 1.5))
                continue

            df = transform(records, ymd)
            upload_to_bq(df)

            logger.info(f"{ymd}: inserted {len(df)} rows")

        except Exception as e:
            logger.error(f"{ymd}: failed - {e}")
            failed_days.append(ymd)

            time.sleep(random.uniform(3.0, 6.0))

    logger.info(f"Collection finished. Failed days: {len(failed_days)}")
    if failed_days:
        logging.info(failed_days[:20])
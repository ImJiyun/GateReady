import re
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

# 프로젝트 루트(src) 경로 추가 (직접 실행 및 모듈 참조용)
src_path = str(Path(__file__).resolve().parent.parent)
if src_path not in sys.path:
    sys.path.append(src_path)

from bq import load_df_to_bq, get_bq_client
from config import BRONZE_FLIGHTS_TABLE_ID, SILVER_FLIGHTS_SNAPSHOTS_TABLE_ID

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

def clean_flight_time(time_str):
    """
    '13:68' -> '1408', '91:2' -> '0912' 등으로 정제
    """
    if not time_str or not isinstance(time_str, str):
        return None

    digits = re.sub(r'[^0-9]', '', time_str)
    if not digits: return None
    
    if len(digits) == 3:
        digits = digits.zfill(4)
    elif len(digits) > 4:
        digits = digits[:4]
    elif len(digits) < 3:
        return None

    try:
        h, m = int(digits[:2]), int(digits[2:])
        if m >= 60:
            h += (m // 60)
            m = m % 60
        h = h % 24
        return f"{h:02d}{m:02d}"
    except (ValueError, TypeError) as e:
        logger.debug(f"Failed to parse time from '{digits}': {e}")
        return None

def process_silver_layer(ymd_list=None):
    """
    Bronze의 모든 이력을 가져되, 파이썬으로 정제(13:68 처리 등)하고
    의미 있는 변화가 있는 스냅샷만 Silver에 저장합니다.
    """
    query = f"SELECT * FROM `{BRONZE_FLIGHTS_TABLE_ID}`"
    if ymd_list:
        ymds = ", ".join([f"'{y}'" for y in ymd_list])
        query += f" WHERE ymd IN ({ymds})"
    
    logger.info("Reading data from Bronze for history analysis...")
    df = pd.read_gbq(query, project_id=BRONZE_FLIGHTS_TABLE_ID.split('.')[0])
    
    if df.empty: return

    # 1. 기괴한 시간 데이터 정제 (13:68 -> 1408)
    df['clean_scheduled'] = df['scheduled_time'].apply(clean_flight_time)
    df['clean_expected'] = df['expected_time'].apply(clean_flight_time)
    df['clean_actual'] = df['actual_time'].apply(clean_flight_time)
    
    # 2. DateTime 변환 (KST -> UTC) 
    for col_prefix in ['scheduled', 'expected', 'actual']:
        datetime_str = df['ymd'] + df[f'clean_{col_prefix}']
        naive_dt = pd.to_datetime(datetime_str, format='%Y%m%d%H%M', errors='coerce')
        df[f'{col_prefix}_utc'] = naive_dt.dt.tz_localize(KST).dt.tz_convert('UTC')

    # 3. 현재 시점의 지연 시간 계산 (우선순위)
    # 1순위: actual 있으면 → actual - scheduled (확정 지연)
    # 2순위: expected 있고, collected > expected → collected - scheduled (expected 지났는데 미출발)
    # 3순위: expected 있고, collected <= expected → expected - scheduled (항공사 예상 지연)
    # 4순위: expected 없고, collected > scheduled → collected - scheduled (예정 시각 지남)
    # 그 외: NULL (아직 예정 시각 전, 정보 없음)


    conditions = [
        df['actual_utc'].notna(),
        df['expected_utc'].notna() & (df['collected_at'] > df['expected_utc']),
        df['expected_utc'].notna() & (df['collected_at'] <= df['expected_utc']),
        df['expected_utc'].isna() & (df['collected_at'] > df['scheduled_utc']),
    ]
    choices = [
        (df['actual_utc'] - df['scheduled_utc']).dt.total_seconds() / 60,
        (df['collected_at'] - df['scheduled_utc']).dt.total_seconds() / 60,
        (df['expected_utc'] - df['scheduled_utc']).dt.total_seconds() / 60,
        (df['collected_at'] - df['scheduled_utc']).dt.total_seconds() / 60,
    ]
    df['current_delay_min'] = np.select(conditions, choices, default=np.nan)

    # 4. 중복 스냅샷 제거 
    df = df.sort_values(['flight_key', 'collected_at'])
    df['prev_expected'] = df.groupby('flight_key')['expected_utc'].shift(1)
    df['prev_status'] = df.groupby('flight_key')['status'].shift(1)

    # 상태가 변했거나, 첫 데이터인 경우만 필터링
    is_changed = (df['expected_utc'] != df['prev_expected']) | (df['status'] != df['prev_status'])
    history_df = df[is_changed].copy()

    # 5. Silver History 테이블에 MERGE (중복 방지)
    if history_df.empty:
        logger.info("No new changes to merge.")
        return

    # 필요한 컬럼만 선택
    target_columns = [
        'flight_key', 'airline_icao', 'flight_iata', 'scheduled_utc', 
        'expected_utc', 'actual_utc', 'status', 'current_delay_min', 
        'collected_at', 'ymd'
    ]
    final_df = history_df[target_columns].copy()

    # client는 미리 만들고, staging_table_id는 None으로 시작
    client = get_bq_client()
    staging_table_id = None

    try:
        # 실행마다 유니크한 staging 테이블 이름 생성
        run_id = datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S")
        staging_table_id = f"{SILVER_FLIGHTS_SNAPSHOTS_TABLE_ID}_staging_{run_id}"

        # 5-1. 데이터를 임시 테이블에 적재 (덮어쓰기)
        logger.info(f"Uploading {len(final_df)} rows to staging table...")
        load_df_to_bq(final_df, staging_table_id, write_disposition="WRITE_TRUNCATE")

        # 5-2. MERGE 쿼리 실행
        merge_query = f"""
        MERGE `{SILVER_FLIGHTS_SNAPSHOTS_TABLE_ID}` T
        USING `{staging_table_id}` S
        ON T.flight_key = S.flight_key AND T.collected_at = S.collected_at
        WHEN NOT MATCHED THEN
          INSERT (flight_key, airline_icao, flight_iata, scheduled_utc, expected_utc, actual_utc, status, current_delay_min, collected_at, ymd)
          VALUES (S.flight_key, S.airline_icao, S.flight_iata, S.scheduled_utc, S.expected_utc, S.actual_utc, S.status, S.current_delay_min, S.collected_at, S.ymd)
        """

        logger.info("Executing MERGE into production Silver table...")
        client.query(merge_query).result()

        logger.info("Silver layer processing completed successfully with MERGE.")

    except Exception:
        logger.exception("Failed to merge Silver layer")
        raise

    finally:
        # 5-3. 성공/실패 상관없이 staging 정리
        if staging_table_id:
            client.delete_table(staging_table_id, not_found_ok=True)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_silver_layer()
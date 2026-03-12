import re
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from src.bq import load_df_to_bq, get_bq_client
from src.config import BRONZE_FLIGHTS_TABLE_ID, SILVER_FLIGHTS_SNAPSHOTS_TABLE_ID

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

# IATA 지연코드 기반 대분류 매핑
DELAY_REASON_CATEGORY_MAP = {
    # 연결편 지연 (R계열 연결, N계열 접속)
    'RA': '연결편 지연',  # 항공기 연결(도착지연)
    'RL': '연결편 지연',  # 화물 연결
    'RC': '연결편 지연',  # 승무원 교체(연결)
    'RS': '연결편 지연',  # 객실 승무원 연결
    'RT': '연결편 지연',  # 환승 수속 오류
    'RX': '연결편 지연',  # 기타(접속 관련)
    'RO': '연결편 지연',  # 운항통제(항로변경, 회항 등)
    'NB': '연결편 지연',  # 정시 출발하였으나 접속 지연
    'NP': '연결편 지연',  # 운송(PO) 사유 접속 지연
    'NL': '연결편 지연',  # 영업 사유 접속 지연
    # 기상 (W계열, AW, NW)
    'WI': '기상',  # 출발공항 제방빙
    'WC': '기상',  # 목적공항 시정악화
    'WX': '기상',  # 기타 기상 악화
    'WD': '기상',  # 목적공항 강한바람
    'WN': '기상',  # 목적공항 눈
    'WG': '기상',  # 출발공항 지상조업(악기상)
    'WR': '기상',  # 항공로 또는 대체공항(기상)
    'AW': '기상',  # ATFM(목적공항 기상)
    'NW': '기상',  # 기상 사유 접속 지연
    # 항공기 정비 (T계열, FM, NT, ND)
    'TN': '항공기 정비',  # 항공기 정비(계획되지 않은)
    'TM': '항공기 정비',  # 항공기 정비(스케줄 지연)
    'TC': '항공기 정비',  # 항공기 교체(기술적 결함)
    'TD': '항공기 정비',  # 항공기 결함
    'TL': '항공기 정비',  # 대체 항공기 부족
    'TR': '항공기 정비',  # 항공기 문제로 돌아옴
    'TX': '항공기 정비',  # 기타(항공기 정비 관련)
    'TS': '항공기 정비',  # 대체품, 정비장비
    'FM': '항공기 정비',  # 기장 요청에 따른 정비
    'NT': '항공기 정비',  # 정비 사유 접속 지연
    'ND': '항공기 정비',  # 항공기 결함 사유 접속 지연
    # 승무원 (F계열)
    'FL': '승무원',  # 객실승무원 지연 탑승
    'FT': '승무원',  # 승무원 탑승 및 출발수속 지연
    'FA': '승무원',  # 규정 미 충족(객실 승무원)
    'FR': '승무원',  # 규정 미 충족(운항승무원)
    'FX': '승무원',  # 기타(운항기준 및 승무원 관련)
    'FS': '승무원',  # 운항승무원 부족
    'FF': '승무원',  # 운항 필수요건 요청
    # 승객·수화물 (P계열)
    'PH': '승객·수화물',  # 탑승(승객 불일치 등)
    'PN': '승객·수화물',  # 미 탑승객 수화물 처리
    'PB': '승객·수화물',  # 위탁수화물처리
    'PA': '승객·수화물',  # 코드셰어 승객 지연
    'PX': '승객·수화물',  # 기타(여객 및 위탁수화물 관련)
    'PW': '승객·수화물',  # 교통약자 탑승 및 하기 처리
    'PE': '승객·수화물',  # 수속오류
    'PS': '승객·수화물',  # 승객편의, VIP 등
    'PC': '승객·수화물',  # 기내식 주문
    # 공항·관제 (A계열, G계열, 시스템, NA/NS/NG)
    'AT': '공항·관제',  # ATFM(항공로)
    'AM': '공항·관제',  # 출발공항 제한사항
    'AX': '공항·관제',  # ATFM(인접국가)
    'AE': '공항·관제',  # ATFM(목적공항)
    'AD': '공항·관제',  # 목적공항 제한사항
    'AF': '공항·관제',  # 공항시설
    'AS': '공항·관제',  # 보안검색
    'AG': '공항·관제',  # 출입국, 세관, 검역
    'AA': '공항·관제',  # 탑승구 부족
    'GC': '공항·관제',  # 항공기 청소
    'GL': '공항·관제',  # 수화물 조업지연
    'GF': '공항·관제',  # 급유 및 배유
    'GE': '공항·관제',  # 조업장비
    'GT': '공항·관제',  # 조업장비(견인차량)
    'GB': '공항·관제',  # 기내식(배송 또는 탑재)
    'SG': '공항·관제',  # 최소지상시간
    'CC': '공항·관제',  # 수속지연
    'EF': '공항·관제',  # 비행 계획 시스템
    'ED': '공항·관제',  # 출발 시스템 오류
    'NA': '공항·관제',  # 공항, 관제 사유 접속 지연
    'NS': '공항·관제',  # 보안 사유 접속 지연
    'NG': '공항·관제',  # 지상조업 사유 접속 지연
    # 기타
    'MX': '기타',   # 기타(지연코드 외)
    'CX': '기타',   # 기타(화물 관련)
    'CO': '기타',   # 화물초과판매
    'CP': '기타',   # 배송지연
    'SP': '기타',   # 미분류
    'SW': '기타',   # 미분류
    'XXX': '기타',  # 기타
}


def get_delay_reason_category(code):
    if pd.isna(code) or not isinstance(code, str) or not code.strip():
        return None
    return DELAY_REASON_CATEGORY_MAP.get(code.strip().upper(), '기타')

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

    # 4. 지연 사유 대분류 매핑
    df['delay_reason_category'] = df['status_remark_code'].apply(get_delay_reason_category)

    # 5. 중복 스냅샷 제거 
    df = df.sort_values(['flight_key', 'collected_at'])
    df['prev_expected'] = df.groupby('flight_key')['expected_utc'].shift(1)
    df['prev_status'] = df.groupby('flight_key')['status'].shift(1)
    df['prev_status_remark'] = df.groupby('flight_key')['status_remark'].shift(1)

    # NaN-safe 변경 감지: pandas에서 NaN != NaN은 True를 반환하므로 명시적 처리 필요
    # Timestamp: 둘 다 NaN이면 변경 없음으로 처리
    expected_changed = ~(
        (df['expected_utc'] == df['prev_expected']) |
        (df['expected_utc'].isna() & df['prev_expected'].isna())
    )
    # String: fillna('')로 NaN을 동일 값으로 치환 후 비교
    status_changed = df['status'].fillna('') != df['prev_status'].fillna('')
    remark_changed = df['status_remark'].fillna('') != df['prev_status_remark'].fillna('')

    is_changed = expected_changed | status_changed | remark_changed
    history_df = df[is_changed].copy()

    # 5. Silver History 테이블에 MERGE (중복 방지)
    if history_df.empty:
        logger.info("No new changes to merge.")
        return

    # 필요한 컬럼만 선택
    target_columns = [
        'flight_key', 'airline_icao', 'airline_kr', 'flight_iata',
        'arr_airport_iata', 'arr_airport_kr', 'nature',
        'scheduled_utc', 'expected_utc', 'actual_utc',
        'status', 'status_remark', 'status_remark_code', 'delay_reason_category',
        'current_delay_min', 'collected_at', 'ymd'
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
          INSERT (flight_key, airline_icao, airline_kr, flight_iata, arr_airport_iata, arr_airport_kr, nature, scheduled_utc, expected_utc, actual_utc, status, status_remark, status_remark_code, delay_reason_category, current_delay_min, collected_at, ymd)
          VALUES (S.flight_key, S.airline_icao, S.airline_kr, S.flight_iata, S.arr_airport_iata, S.arr_airport_kr, S.nature, S.scheduled_utc, S.expected_utc, S.actual_utc, S.status, S.status_remark, S.status_remark_code, S.delay_reason_category, S.current_delay_min, S.collected_at, S.ymd)
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
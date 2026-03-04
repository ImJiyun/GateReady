"""
Silver Layer Cloud Run Job 진입점
Cloud Workflows에 의해 Bronze Job 완료 후 순차적으로 실행됨
"""
import sys
import logging
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.logger import setup_logging
from src.collectors.silver import process_silver_layer
from src.bq import get_bq_client
from src.config import BRONZE_FLIGHTS_TABLE_ID


def get_target_ymds(lookback_minutes: int = 15) -> list[str]:
    """
    Bronze 테이블을 직접 쿼리하여 최근 수집된 데이터의 대상 날짜(ymd)를 가져옵니다.
    이는 Bronze/Silver Job 실행 시점 차이로 인한 데이터 누락을 방지합니다.
    """
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT ymd
        FROM `{BRONZE_FLIGHTS_TABLE_ID}`
        WHERE collected_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_minutes} MINUTE)
        ORDER BY ymd
    """
    logging.info(f"Querying for target ymds with lookback: {lookback_minutes} minutes")
    try:
        rows = client.query(query).result()
        ymds = [row.ymd for row in rows]
        if not ymds:
            logging.warning("No recent data found in Bronze table for the given lookback window.")
        return ymds
    except Exception:
        logging.exception("Failed to query target ymds from BigQuery")
        return []


def main():
    setup_logging("silver_job")

    target_ymds = get_target_ymds()
    logging.info(f"Starting Silver Job execution for dates: {target_ymds}")
    try:
        process_silver_layer(target_ymds)
        logging.info("Silver Job execution completed successfully.")
    except Exception:
        logging.exception("Silver Job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

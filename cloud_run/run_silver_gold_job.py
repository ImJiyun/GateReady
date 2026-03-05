"""
Silver + Gold 통합 Cloud Run Job 진입점
매일 오전 5시(KST) Cloud Scheduler → Cloud Workflows에 의해 실행됩니다.

실행 순서:
  1. Silver: 전날 ymd 데이터 전체를 Bronze에서 정제 → silver.flights_snapshots에 MERGE
  2. Gold:   Silver 결과를 기반으로 Tableau용 Gold 테이블 두 개를 CREATE OR REPLACE
"""
import sys
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.logger import setup_logging
from src.collectors.silver import process_silver_layer
from src.collectors.gold import process_gold_layer

KST = ZoneInfo("Asia/Seoul")


def get_yesterday_ymd() -> str:
    """KST 기준 전날 날짜를 'YYYYMMDD' 형식으로 반환합니다."""
    yesterday = datetime.now(KST) - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")


def main():
    setup_logging("silver_gold_job")

    yesterday_ymd = get_yesterday_ymd()
    logging.info(f"=== Silver+Gold Job started | target date: {yesterday_ymd} ===")

    # ── Step 1: Silver ────────────────────────────────────────    
    logging.info("[1/2] Starting Silver layer processing...")
    try:
        process_silver_layer(ymd_list=[yesterday_ymd])
        logging.info("[1/2] Silver layer processing completed.")
    except Exception:
        logging.exception("[1/2] Silver layer processing failed — aborting job")
        sys.exit(1)

    # ── Step 2: Gold ──────────────────────────────────────────
    logging.info("[2/2] Starting Gold layer processing...")
    try:
        process_gold_layer()
        logging.info("[2/2] Gold layer processing completed.")
    except Exception:
        logging.exception("[2/2] Gold layer processing failed")
        sys.exit(1)

    logging.info("=== Silver+Gold Job completed ===")


if __name__ == "__main__":
    main()

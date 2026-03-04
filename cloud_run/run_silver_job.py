"""
Silver Layer Cloud Run Job 진입점
Cloud Workflows에 의해 Bronze Job 완료 후 순차적으로 실행됨
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.logger import setup_logging
from src.collectors.silver import process_silver_layer

KST = ZoneInfo("Asia/Seoul")


def get_target_ymds() -> list[str]:
    """
    Bronze 수집 윈도우(현재 시각 ±3시간)와 동일한 날짜 범위를 반환.
    자정 근처엔 어제 날짜도 포함될 수 있음.
    """
    now = datetime.now(KST)
    start_dt = now - timedelta(hours=3)
    end_dt = now + timedelta(hours=3)

    ymds = {start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")}
    return sorted(ymds)


def main():
    setup_logging("silver_job")

    target_ymds = get_target_ymds()
    logging.info(f"Starting Silver Job execution for dates: {target_ymds}")
    try:
        process_silver_layer(target_ymds)
        logging.info("Silver Job execution completed successfully.")
    except Exception as e:
        logging.error(f"Silver Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

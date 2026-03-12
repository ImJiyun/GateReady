"""
Gold Layer 처리 모듈
Silver 데이터를 기반으로 Tableau용 Gold 테이블 두 개를 갱신합니다.
- gold.tableau_flights_dashboard   : 항공편별 최신 운항 현황
- gold.tableau_delay_escalation    : 항공편별 최초→최종 지연 변화 분석

Scheduled Query 대신 Cloud Run Job(silver-gold-job)에 의해 매일 오전 5시 실행됩니다.
"""
import logging
from pathlib import Path

from src.bq import get_bq_client
from src.config import (
    BQ_PROJECT_ID,
    BQ_DATASET_SILVER,
    BQ_DATASET_GOLD,
)

logger = logging.getLogger(__name__)

# 프로젝트 루트 기준으로 SQL 디렉토리 경로 계산
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_SQL_DIR = _PROJECT_ROOT / "sql" / "gold"


def _load_sql(filename: str) -> str:
    """
    SQL 파일을 읽고, 파일 내 dataset 참조를 실제 project_id 포함 전체 경로로 치환합니다.
    """
    sql = (_SQL_DIR / filename).read_text(encoding="utf-8")
    sql = sql.replace("`gold.",   f"`{BQ_PROJECT_ID}.{BQ_DATASET_GOLD}.")
    sql = sql.replace("`silver.", f"`{BQ_PROJECT_ID}.{BQ_DATASET_SILVER}.")
    return sql


def process_gold_layer():
    """Gold 레이어 두 테이블을 순서대로 갱신합니다."""
    client = get_bq_client()

    jobs = [
        ("tableau_flights_dashboard", "create_tableau_dashboard_table.sql"),
        ("tableau_delay_escalation",  "create_delay_escalation_table.sql"),
    ]

    for table_name, sql_file in jobs:
        logger.info(f"Building Gold table: {table_name}")
        try:
            sql = _load_sql(sql_file)
            query_job = client.query(sql)
            query_job.result()
            logger.info(f"Gold table '{table_name}' created/replaced successfully.")
        except Exception:
            logger.exception(f"Failed to build Gold table: {table_name}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_gold_layer()

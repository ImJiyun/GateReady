import sys
from pathlib import Path
from google.cloud import bigquery
import logging

# 프로젝트 루트를 sys.path에 추가하여 src 패키지를 찾을 수 있게 함
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.logger import setup_logging
from src.config import BQ_PROJECT_ID

logger = logging.getLogger(__name__)

def execute_sql_file(client: bigquery.Client, sql_file: Path, dry_run: bool = False):
    """SQL 파일 실행 (PROJECT_ID 치환)"""
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    # PROJECT_ID를 실제 프로젝트 ID로 치환
    sql = sql.replace('PROJECT_ID', client.project)
    
    logger.info(f"Executing {sql_file.name}")
    
    if dry_run:
        logger.info(sql)
        return
    
    try:
        job = client.query(sql)
        job.result()
        logger.info(f"Successfully executed {sql_file.name}")
    except Exception:
        logger.exception(f"Error executing {sql_file.name}")
        raise

def main():
    # 로깅 초기화
    setup_logging()
    
    if not BQ_PROJECT_ID:
        raise ValueError("BQ_PROJECT_ID not set in .env file")
    
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # SQL 파일 실행
    sql_files = [
        # Bronze
        'sql/bronze/create_flights_table.sql',
        'sql/bronze/create_airlines_table.sql',
        # Silver
        'sql/silver/create_flights_snapshots_table.sql',
        'sql/silver/create_flights_view.sql',
        # Gold
        'sql/gold/create_tableau_dashboard_table.sql',
        'sql/gold/create_delay_escalation_table.sql'
    ]
    
    for sql_file in sql_files:
        path = Path(sql_file)
        if path.exists():
            execute_sql_file(client, path)

if __name__ == '__main__':
    main()
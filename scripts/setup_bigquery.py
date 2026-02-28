import sys
from pathlib import Path
from google.cloud import bigquery
import logging

# 프로젝트 루트를 sys.path에 추가하여 src 패키지를 찾을 수 있게 함
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.config import BQ_PROJECT_ID, BQ_DATASET_BRONZE

def execute_sql_file(client: bigquery.Client, sql_file: Path, dry_run: bool = False):
    """SQL 파일 실행 (PROJECT_ID 치환)"""
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    # PROJECT_ID를 실제 프로젝트 ID로 치환
    sql = sql.replace('PROJECT_ID', client.project)
    
    logging.info(f"\n{'='*60}")
    logging.info(f"File: {sql_file.name}")
    logging.info(f"Project: {client.project}")
    logging.info(f"{'='*60}")
    
    if dry_run:
        logging.info(sql)
        return
    
    try:
        job = client.query(sql)
        job.result()
        logging.info(f"Success")
    except Exception as e:
        logging.info(f"Error: {e}")
        raise

def main():
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    if not BQ_PROJECT_ID:
        raise ValueError("BQ_PROJECT_ID not set in .env file")
    
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # SQL 파일 실행
    sql_files = [
        'sql/bronze/create_flights_table.sql',
    ]
    
    for sql_file in sql_files:
        path = Path(sql_file)
        if path.exists():
            execute_sql_file(client, path)

if __name__ == '__main__':
    main()
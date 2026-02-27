import os
from pathlib import Path
from dotenv import load_dotenv

# .env 로드
load_dotenv(override=True)

# BigQuery
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE")

# GOOGLE_APPLICATION_CREDENTIALS가 상대 경로일 경우 절대 경로로 변환
creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if creds_path and not os.path.isabs(creds_path):
    project_root = Path(__file__).resolve().parent.parent
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(project_root / creds_path.strip('"').strip("'"))

BRONZE_FLIGHTS_TABLE_ID = f"{BQ_PROJECT_ID}.{BQ_DATASET_BRONZE}.flights"
BRONZE_AIRLINES_TABLE_ID = f"{BQ_PROJECT_ID}.{BQ_DATASET_BRONZE}.airlines"

# API
FLIGHT_API_URL = os.getenv("FLIGHT_API_URL")
AIRLINE_API_URL = os.getenv("AIRLINE_API_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.airportal.go.kr/airport/aircraftInfo.do",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/json;charset=UTF-8",
}

DEFAULT_API_TIMEOUT = 20
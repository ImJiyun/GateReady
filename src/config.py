import os

# GCP
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "flight_delay"
RAW_FLIGHTS_TABLE_ID = f"{GCP_PROJECT_ID}.{DATASET_ID}.raw_flights"
AIRLINES_TABLE_ID = f"{GCP_PROJECT_ID}.{DATASET_ID}.airlines"

# API
FLIGHT_API_URL = os.getenv("FLIGHT_API_URL")
AIRLINE_API_URL = os.getenv("AIRLINE_API_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.airportal.go.kr/airport/aircraftInfo.do",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/json;charset=UTF-8",
}
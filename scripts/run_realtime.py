import schedule
import time
from datetime import datetime
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.logger import setup_logging
from src.collectors.realtime import collect_realtime

# 로깅 초기화
setup_logging("realtime")

def job():
    logging.info(f"Starting job...")
    try:
        collect_realtime()
    except Exception as e:
        logging.error(f"Job failed: {e}")

# 즉시 한번 실행
job()

# 10분마다 스케줄
schedule.every(10).minutes.do(job)

logging.info("Realtime collector started (every 10 min)")

while True:
    schedule.run_pending()
    time.sleep(60)
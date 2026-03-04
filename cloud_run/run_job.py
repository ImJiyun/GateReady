import sys
import logging

from src.logger import setup_logging
from src.collectors.realtime import collect_realtime

def main():
    # Initialize logging
    setup_logging("realtime_job")
    
    logging.info("Starting Realtime Job execution (from cloud_run folder)...")
    try:
        collect_realtime()
        logging.info("Realtime Job execution completed successfully.")
    except Exception:
        logging.exception("Realtime Job failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

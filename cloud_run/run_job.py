import sys
import logging
from pathlib import Path

# Add project root to sys.path
# Now in cloud_run/run_job.py, so parent is root
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.logger import setup_logging
from src.collectors.realtime import collect_realtime

def main():
    # Initialize logging
    setup_logging("realtime_job")
    
    logging.info("Starting Realtime Job execution (from cloud_run folder)...")
    try:
        collect_realtime()
        logging.info("Realtime Job execution completed successfully.")
    except Exception as e:
        logging.error(f"Realtime Job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

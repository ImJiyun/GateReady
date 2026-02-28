import sys
from pathlib import Path
import logging

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.logger import setup_logging
from src.collectors.bronze import collect_bronze_range

def main():
    # 1. 로깅 초기화 (콘솔 출력 + 파일 저장 시작)
    setup_logging("bronze")
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Bronze data collection script")
    
    # 2. 수집 범위 설정
    start_date = "2023-01-01"
    end_date = "2025-12-31"
    
    try:
        # 3. 실제 수집 함수 호출
        collect_bronze_range(start_date, end_date)
        logger.info("Bronze data collection completed successfully")
    except Exception:
        logger.exception("Critical error during bronze collection")
        sys.exit(1)

if __name__ == "__main__":
    main()
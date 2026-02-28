"""
Silver Layer 수동 실행 스크립트
과거 데이터를 재처리하거나 실시간 수집과 별개로 정제를 수행할 때 사용합니다.
"""
import sys
from pathlib import Path
import logging

# 프로젝트 루트 경로 추가
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from src.collectors.silver import process_silver_layer

def main():
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    # 1. 인자가 있으면 특정 날짜 목록 처리 (예: python run_silver.py 20240301 20240302)
    if len(sys.argv) > 1:
        ymd_list = sys.argv[1:]
        logger.info(f"Manually processing Silver layer for dates: {ymd_list}")
        process_silver_layer(ymd_list)
    # 2. 인자가 없으면 전체(또는 최근 데이터) 처리
    else:
        logger.info("Manually processing Silver layer for all available Bronze data...")
        process_silver_layer()

if __name__ == "__main__":
    main()

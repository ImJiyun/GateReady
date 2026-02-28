import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

def setup_logging(log_name="app", level=logging.INFO):
    """
    프로젝트 전체의 로깅 설정을 초기화합니다.
    - log_name별로 파일 분리
    - 매일 자정 파일 순환
    - 최근 30일치 로그만 보관
    """
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # 로그 디렉토리 생성
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 이미 헨들러가 설정되어 있다면 중복 방지를 위해 제거
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # 핸들러 1: 콘솔 출력
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(console_handler)

    # 핸들러 2: 파일 출력
    file_handler = TimedRotatingFileHandler(
        log_dir / f"{log_name}.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    # 백업 파일명 형식 설정
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)



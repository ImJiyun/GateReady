import logging
import sys

def setup_logging(log_name: str = "app", level: int = logging.INFO) -> None:
    """
    프로젝트 전체의 로깅 설정을 초기화합니다.

    Cloud Run 환경에서는 stdout이 Google Cloud Logging과 자동으로 통합되므로
    콘솔 핸들러만 사용합니다. 파일 핸들러는 사용하지 않습니다.
    (컨테이너 종료 시 파일 시스템이 사라지기 때문)
    """
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # 이미 핸들러가 설정되어 있다면 중복 방지를 위해 제거
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # 콘솔 핸들러 (stdout → Cloud Logging 자동 수집)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(console_handler)

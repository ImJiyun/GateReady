# 1. Python 공식 이미지 사용 (가벼운 slim 버전)
FROM python:3.11-slim

# 2. 작업 디렉토리 설정
WORKDIR /app

# 3. 필수 패키지 설치 및 캐시 삭제
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 4. 의존성 파일 복사 및 설치
# 빌드 시 컨텍스트(. )를 루트로 줄 것이므로 .에서 찾음
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 프로젝트 소스 코드 복사 (전체 복사)
COPY . .

# 6. 로그 디렉토리 미리 생성 (권한 대비)
RUN mkdir -p logs && chmod 777 logs

# 7. Cloud Run Job 실행 시 동작할 스크립트 지정
# 복사된 cloud_run 폴더 안의 run_job.py를 실행
CMD ["python", "cloud_run/run_job.py"]

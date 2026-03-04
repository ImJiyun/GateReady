# ✈️ GateReady — 항공편 지연 분석 파이프라인

> 인천공항 출발 항공편의 실시간 지연 데이터를 수집·가공하여  
> **지연이 발생했을 때 추가 지연 시간이 얼마나 더 쌓이는지**를 분석하는 데이터 파이프라인 프로젝트입니다.

## 프로젝트 목적

항공편이 지연되면 최초 지연 공지 이후에도 예정 출발 시각이 계속 밀리는 경우가 많습니다.  
이 프로젝트는 매 10분마다 항공편 상태를 스냅샷으로 수집하여 **지연이 공지된 이후 실제로 추가 지연이 얼마나 발생하는지**를 추적합니다.

## 분석 목표

- 지연이 **처음 공지된 시점**과 **실제 출발 시각** 사이의 추가 지연 분포
- 항공사별, 시간대별 지연 패턴 비교
- 지연이 발생한 항공편이 "제시간에 맞춰지는" 비율 vs "더 늦어지는" 비율

## 기술 스택

- **수집**: Python, Requests (인천공항 공개 API)
- **컨테이너**: Docker, Google Cloud Run Jobs
- **오케스트레이션**: Google Cloud Workflows
- **스케줄링**: Google Cloud Scheduler
- **저장소**: Google BigQuery
- **CI/CD**: Google Cloud Build
- **시각화**: Tableau

## 아키텍처

![Architecture](./docs/architecture.png)

### 레이어 구조

| 레이어     | 테이블                     | 설명                                                    |
| ---------- | -------------------------- | ------------------------------------------------------- |
| **Bronze** | `bronze.flights`           | API 원본 그대로 적재. 수집 시각(±3시간 윈도우)별 스냅샷 |
| **Silver** | `silver.flights_snapshots` | 상태 변화가 있는 스냅샷만 필터링 + 이상 시간값 정제     |
| **Gold**   | (Tableau 연결)             | 지연 패턴 분석용 집계 테이블                            |

### ERD

![ERD](./docs/ERD.png)

## 설계 포인트

### ±3시간 수집 윈도우

매 실행마다 현재 시각 기준 **앞뒤 3시간** 범위의 항공편을 조회합니다.  
단순히 "지금 시각의 항공편"만 보면, 지연된 항공편의 이력 변화를 충분히 추적할 수 없기 때문입니다.

### Silver 레이어의 MERGE 전략

Silver 적재 시 단순 `WRITE_APPEND`가 아닌 **staging 테이블 → MERGE** 방식을 사용합니다.  
동일 `(flight_key, collected_at)` 조합이 중복으로 들어오는 것을 방지하여 멱등성을 확보합니다.

### 이상 시간값 처리

API 응답에 `"13:68"`, `"91:2"` 같은 시간 형식이 혼재합니다.  
공항 시스템 내부의 포맷 불일치로 추정되며, 이를 숫자로만 받아 직접 파싱하는 `clean_flight_time()` 로직을 구현했습니다.

## 프로젝트 구조

```
GateReady/
├── cloud_run/
│   ├── Dockerfile            # Bronze Job 이미지
│   ├── Dockerfile.silver     # Silver Job 이미지
│   ├── run_job.py            # Bronze Job 엔트리포인트
│   └── run_silver_job.py     # Silver Job 엔트리포인트
├── src/
│   ├── collectors/
│   │   ├── bronze.py         # API 수집 + BigQuery 적재
│   │   ├── realtime.py       # 실시간 수집 (±3시간 윈도우)
│   │   └── silver.py         # Bronze 정제 + MERGE
│   ├── bq.py                 # BigQuery 클라이언트 공통 모듈
│   └── config.py             # 환경변수 기반 설정
├── workflows/
│   └── pipeline.yaml         # Cloud Workflows 정의 (Bronze → Silver)
├── cloudbuild.yaml           # Cloud Build CI/CD
└── sql/                      # Gold 레이어용 쿼리
```

## 배포

```bash
# 전체 빌드 및 배포 (Bronze + Silver 이미지 + Workflows)
gcloud builds submit --config cloudbuild.yaml --project=$PROJECT_ID .
```

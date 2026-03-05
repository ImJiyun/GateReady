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

| 역할     | 기술                                 |
| -------- | ------------------------------------ |
| 수집     | Python, Requests (인천공항 공개 API) |
| 컨테이너 | Docker, Google Cloud Run Jobs        |
| 스케줄링 | Google Cloud Scheduler               |
| 저장소   | Google BigQuery                      |
| CI/CD    | Google Cloud Build                   |
| 시각화   | Tableau                              |

## 아키텍처

![Architecture](./docs/architecture.png)

### 레이어 구조

| 레이어     | 테이블                           | 갱신 주기  | 설명                                                    |
| ---------- | -------------------------------- | ---------- | ------------------------------------------------------- |
| **Bronze** | `bronze.flights`                 | 매 10분    | API 원본 그대로 적재. 수집 시각(±3시간 윈도우)별 스냅샷 |
| **Silver** | `silver.flights_snapshots`       | 매일 05:00 | 상태 변화가 있는 스냅샷만 필터링 + 이상 시간값 정제     |
| **Gold**   | `gold.tableau_flights_dashboard` | 매일 05:00 | 항공편별 최신 운항 현황 (Tableau 연결)                  |
| **Gold**   | `gold.tableau_delay_escalation`  | 매일 05:00 | 항공편별 최초→최종 지연 변화 분석 (Tableau 연결)        |

### ERD

![ERD](./docs/ERD.png)

## 설계 포인트

### ±3시간 수집 윈도우

매 실행마다 현재 시각 기준 **앞뒤 3시간** 범위의 항공편을 조회합니다.  
단순히 "지금 시각의 항공편"만 보면, 지연된 항공편의 이력 변화를 충분히 추적할 수 없기 때문입니다.

### Silver 레이어의 MERGE 전략

Silver 적재 시 단순 `WRITE_APPEND`가 아닌 **staging 테이블 → MERGE** 방식을 사용합니다.  
동일 `(flight_key, collected_at)` 조합이 중복으로 들어오는 것을 방지하여 멱등성을 확보합니다.

### Silver + Gold 통합 Job

Silver와 Gold를 별도 Job으로 분리하지 않고 **단일 Cloud Run Job(`silver-gold-job`)** 으로 통합했습니다.  
Silver 처리가 완료된 뒤 Gold를 실행하는 순서 보장이 필요하고, 두 작업 모두 하루 1회 실행이라 오케스트레이션 레이어(Cloud Workflows)를 별도로 두는 것이 과도한 복잡성을 추가한다고 판단했습니다.  
Silver 실패 시 Gold 실행을 중단(`raise SystemExit(1)`)하여 불완전한 Gold 데이터 생성을 방지합니다.

### 이상 시간값 처리

API 응답에 `"13:68"`, `"91:2"` 같은 시간 형식이 혼재합니다.  
공항 시스템 내부의 포맷 불일치로 추정되며, 이를 숫자로만 받아 직접 파싱하는 `clean_flight_time()` 로직을 구현했습니다.

## 프로젝트 구조

```
GateReady/
├── cloud_run/
│   ├── Dockerfile              # Bronze Job 이미지 (realtime-job)
│   ├── Dockerfile.silver_gold  # Silver+Gold 통합 Job 이미지 (silver-gold-job)
│   ├── run_job.py              # Bronze Job 엔트리포인트
│   └── run_silver_gold_job.py  # Silver+Gold Job 엔트리포인트
├── src/
│   ├── collectors/
│   │   ├── bronze.py           # API 수집 + BigQuery 적재
│   │   ├── realtime.py         # 실시간 수집 (±3시간 윈도우)
│   │   ├── silver.py           # Bronze 정제 + MERGE
│   │   └── gold.py             # Silver 기반 Gold 테이블 갱신
│   ├── bq.py                   # BigQuery 클라이언트 공통 모듈
│   ├── config.py               # 환경변수 기반 설정
│   └── logger.py               # 로깅 설정
├── sql/
│   ├── bronze/                 # Bronze 테이블 정의 DDL
│   ├── silver/                 # Silver 테이블 정의 DDL
│   └── gold/
│       ├── create_tableau_dashboard_table.sql   # Gold: 최신 운항 현황
│       └── create_delay_escalation_table.sql    # Gold: 지연 변화 분석
├── cloudbuild.yaml             # Cloud Build CI/CD (Bronze + Silver+Gold 이미지)
└── .env.example                # 환경변수 예시
```

## 배포

```bash
# 전체 빌드 및 배포 (Bronze 이미지 + Silver+Gold 이미지)
gcloud builds submit --config cloudbuild.yaml --project=$PROJECT_ID .
```

Cloud Build는 다음 두 이미지를 빌드하고 Cloud Run Job을 업데이트합니다:

| 이미지                                     | Cloud Run Job     | 트리거                           |
| ------------------------------------------ | ----------------- | -------------------------------- |
| `gcr.io/$PROJECT_ID/realtime-collector`    | `realtime-job`    | Cloud Scheduler (매 10분)        |
| `gcr.io/$PROJECT_ID/silver-gold-collector` | `silver-gold-job` | Cloud Scheduler (매일 05:00 KST) |

## 환경변수

`.env.example`을 참고하여 `.env` 파일을 생성합니다.

| 변수명                           | 설명                                    |
| -------------------------------- | --------------------------------------- |
| `BQ_PROJECT_ID`                  | GCP 프로젝트 ID                         |
| `BQ_DATASET_BRONZE`              | Bronze 데이터셋 이름 (기본값: `bronze`) |
| `BQ_DATASET_SILVER`              | Silver 데이터셋 이름 (기본값: `silver`) |
| `BQ_DATASET_GOLD`                | Gold 데이터셋 이름 (기본값: `gold`)     |
| `FLIGHT_API_URL`                 | 인천공항 항공편 API 엔드포인트          |
| `AIRLINE_API_URL`                | 항공사 정보 API 엔드포인트              |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP 서비스 계정 키 경로                 |

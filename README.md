# ✈️ GateReady — 항공편 지연 분석 파이프라인

> **"지연 공지를 받은 승객은 실제로 얼마나 더 기다려야 하는가?"**  
> 인천공항 출발 항공편을 10분마다 스냅샷으로 수집하여, 최초 지연 공지 이후 추가 지연이 얼마나 더 쌓이는지를 정량화한 데이터 파이프라인 프로젝트입니다.

## 목차

- [프로젝트 목적](#프로젝트-목적)
- [분석 목표](#분석-목표)
- [분석 결과](#분석-결과)
- [기술 스택](#기술-스택)
- [아키텍처](#아키텍처)
- [설계 포인트](#설계-포인트)
- [프로젝트 구조](#프로젝트-구조)
- [환경변수](#환경변수)
- [배포](#배포)
- [한계 및 향후 계획](#한계-및-향후-계획)

## 프로젝트 목적

항공편이 지연되면 최초 지연 공지 이후에도 예정 출발 시각이 계속 밀리는 경우가 많습니다.  
이 프로젝트는 매 10분마다 항공편 상태를 스냅샷으로 수집하여 **지연이 공지된 이후 실제로 추가 지연이 얼마나 발생하는지**를 추적합니다.

## 분석 목표

- 지연이 **처음 공지된 시점**과 **실제 출발 시각** 사이의 추가 지연 분포
- 항공사별, 시간대별 지연 패턴 비교
- 지연이 발생한 항공편이 "제시간에 맞춰지는" 비율 vs "더 늦어지는" 비율

## 분석 결과

> 2026년 2월 28일 수집 시작, 3월 10일 기준 인천공항 출발 여객편 **5,181건** 유효 수집
> (전체 수집 데이터 대비 데이터 품질 97.1%)

**추가 지연 분석** — 지연 공지 이력이 있고 2회 이상 관측된 99건 기준:

| 지표                            | 수치       |
| ------------------------------- | ---------- |
| 지연 공지 후 **추가 악화** 비율 | **74.7%**  |
| 그 중 **15분 이상** 추가 악화   | **30.3%**  |
| 악화된 경우 **평균 추가 지연**  | **17.9분** |
| 변동 없음 (±0분)                | 4.1%       |
| 오히려 **개선**된 비율          | 21.2%      |

→ 지연 공지를 받은 항공편 4건 중 3건은 결국 더 지연됩니다.

**항공사별 패턴**: 악화 빈도가 높은 항공사와 빈도는 낮지만 일단 악화되면 크게 밀리는 항공사로 뚜렷하게 구분되는 경향이 관측됩니다. (표본 부족으로 추가 수집 후 검증 예정)

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

### 수집 윈도우 설계 및 개선

매 실행마다 현재 시각 기준 일정 범위의 항공편을 조회합니다.  
단순히 "지금 시각의 항공편"만 보면, 지연된 항공편의 이력 변화를 충분히 추적할 수 없기 때문입니다.

**초기 설계**: 현재 시각 `±3시간` (예정 출발 기준)

**문제 발견**: 수집된 데이터를 분석한 결과, 지연이 3시간을 초과하는 항공편은 예정 출발 시각이 수집 윈도우 밖으로 밀려나 최종 출발 시각이 누락되는 현상을 확인했습니다.  
2026년 3월 10일 기준, 전체 수집 데이터의 8.9%(475건)에서 `actual_utc`가 `NULL`로 기록되었고, 이 중 96%(457건)가 지연 항공편이었습니다.

**개선**: 현재는 **과거 5시간 / 미래 3시간** 비대칭 윈도우로 변경.  
미래 방향 확장은 의미가 없고(아직 상태 변화가 없는 항공편), 과거 방향을 넓혀 장기 지연 항공편을 더 오래 추적합니다.

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

## 환경변수

`.env.example`을 참고하여 `.env` 파일을 생성합니다. 배포 및 로컬 실행을 위해 먼저 설정되어야 합니다.

| 변수명                           | 설명                                    |
| -------------------------------- | --------------------------------------- |
| `BQ_PROJECT_ID`                  | GCP 프로젝트 ID                         |
| `BQ_DATASET_BRONZE`              | Bronze 데이터셋 이름 (기본값: `bronze`) |
| `BQ_DATASET_SILVER`              | Silver 데이터셋 이름 (기본값: `silver`) |
| `BQ_DATASET_GOLD`                | Gold 데이터셋 이름 (기본값: `gold`)     |
| `FLIGHT_API_URL`                 | 인천공항 항공편 API 엔드포인트          |
| `AIRLINE_API_URL`                | 항공사 정보 API 엔드포인트              |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP 서비스 계정 키 경로                 |

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

## 한계 및 향후 계획

### 수집 윈도우와 장기 지연 항공편

수집 윈도우는 **예정 출발 시각 기준 과거 5시간 ~ 미래 3시간**입니다.  
지연이 5시간을 초과하는 극단적 케이스는 윈도우 밖으로 벗어나 최종 출발 시각이 누락될 수 있습니다.

**개선 방향**: 미출발 지연 항공편 목록을 BigQuery에서 조회하여 항상 수집 대상에 포함하는 watchlist 기반 수집 로직 도입 예정

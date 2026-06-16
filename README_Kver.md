# 🚚 Delivery ETA Prediction Lambda

## 경로최적화 결과와 작업시간 예측 모델을 결합한 배송 ETA 계산 시스템

배송 경로최적화 결과와 이동시간(OSRM), 아이템별 작업시간(`tasu`) 예측 모델을 결합하여 배송 도착 예정 시간(ETA)을 계산하고 TMS에서 조회할 수 있도록 저장하는 Serverless MLOps 프로젝트입니다.

> 본 저장소는 포트폴리오 공개용으로 정리한 버전입니다. 실제 운영 계정, S3 Bucket, Function URL, DB 테이블명, 고객사 정보, 송장번호 등 민감 정보는 샘플 값으로 치환했습니다.

---

## Executive Impact

| Area | Before | After |
|---|---|---|
| ETA 계산 | 이동시간 또는 고정 작업시간 중심 | OSRM 이동시간 + ML 기반 tasu 예측 |
| ETA 갱신 | 수동/제한적 갱신 | 출차/배송완료 이벤트 기반 재계산 |
| 조회 구조 | 직접 계산 필요 | DynamoDB 최신 ETA 조회 API |
| 운영 화면 | 배송 상태 중심 | TMS 지도 marker ETA 표시 |

---

## Business Problem

배송 운영에서는 “현재 기사 기준으로 각 배송지가 언제 도착 예정인지”를 운영팀과 TMS에서 확인할 수 있어야 합니다.  
하지만 ETA를 정확히 계산하려면 단순 거리/시간뿐 아니라 다음 요소를 함께 고려해야 합니다.

- 경로최적화 결과에 따른 방문 순서
- 출차지 → 첫 배송지, 배송지 → 배송지 간 실제 이동시간
- 각 배송지에서 발생하는 아이템별 작업시간(tasu)
- 배송완료 이벤트 이후 남은 배송지 기준 재계산
- 동일 주소/동일 수령지 중복 처리

---

## My Role

- 경로최적화 결과와 ETA 계산 Lambda 연동 구조 설계
- OSRM route API 기반 stop 간 이동시간 계산 로직 구현
- S3 최신 모델 artifact 로딩 및 tasu 예측값 적용
- 출차 이벤트/배송완료 이벤트 기반 ETA 재계산 흐름 구현
- DynamoDB 저장 구조 및 GET 조회 API 구성
- 동일 주소/동일 수령지 중복 ETA 계산 방지 로직 구현
- TMS 적용 화면에서 사용할 수 있는 응답 구조 설계

---

## Architecture

![ETA Calculate Architecture](docs/images/architecture.png)

```text
Flex App / EventBridge
        ↓
Route Optimization Result
        ↓
ETA Calculate Lambda
        ↓
OSRM Travel Time
        ↓
Tasu Model Prediction
        ↓
ETA Accumulation
        ↓
DynamoDB Save
        ↓
TMS ETA Query API
```

---

## End-to-End Flow

1. Flex App에서 출차 또는 배송완료 이벤트 발생
2. 경로최적화 Lambda가 방문 순서를 계산하고 S3에 결과 저장
3. ETA Calculate Lambda가 경로 결과, OSRM 이동시간, tasu 예측값을 조합하여 ETA 계산
4. 계산 결과를 DynamoDB에 저장
5. TMS가 ETA Query Lambda GET API를 호출하여 최신 ETA 조회
6. TMS 지도 marker에 배송지별 ETA 표시

---

## ETA Calculation Logic

ETA는 출차 시각 또는 현재 시각을 기준으로 누적 계산합니다.

```text
ETA(1) = base_time + travel_time(unit → stop_1) + service_time(stop_1)
ETA(n) = ETA(n-1) + travel_time(stop_n-1 → stop_n) + service_time(stop_n)
```

### 주요 처리 기준

- 최초 계산: 출차 시각 기준으로 전체 ETA 생성
- 배송완료 후 재계산: Lambda 실행 시점 기준으로 남은 배송지 ETA 재산출
- 동일 `address_id`를 가진 복수 송장은 하나의 배송지로 간주하여 중복 ETA 방지
- 모델에 없는 sector/area는 default 작업시간으로 fallback
- 작업시간(`tasu`)은 한 지점에서 다음 지점으로 이동한 뒤 하나의 아이템을 배송 처리하는 데 걸리는 시간으로 정의

---

## TMS 적용 화면

TMS 지도 화면에서 각 배송지 marker에 ETA가 표시됩니다.

![TMS ETA Result 1](docs/images/tms_eta_1.png)

![TMS ETA Result 2](docs/images/tms_eta_2.png)

---

## Event Trigger

### 1. 경로최적화 완료 후 ETA 생성

```json
{
  "user_id": 1234,
  "route_s3": {
    "bucket": "sample-route-result-bucket",
    "key": "route-optimization/dt=2026-04-24/user_id=1234/request_id=sample.json"
  }
}
```

### 2. 배송완료 EventBridge 수신 후 ETA 재계산

```json
{
  "detail-type": "ShippingItemExternalNotification",
  "source": "sample.api",
  "detail": {
    "params": {
      "env": "dev",
      "tracking_number": "SAMPLE_TRACKING_NUMBER"
    }
  }
}
```

---

## DynamoDB 저장 구조

| Key | Description |
|---|---|
| `user_id` | 배송 기사 또는 사용자 ID |
| `tracking_number` | 송장번호 또는 item 식별자 |
| `eta_kst` | KST 기준 ETA |
| `ordering` | 경로최적화 방문 순서 |
| `latest_calculated_at` | 마지막 계산 시각 |
| `route_s3` | 기준 경로최적화 결과 S3 위치 |

---

## Tech Stack

| Category | Stack |
|---|---|
| Compute | AWS Lambda, AWS SAM, Container Image |
| Event | Amazon EventBridge |
| Storage | Amazon S3, Amazon DynamoDB |
| Routing | OSRM |
| ML/Data | Python, Pandas, scikit-learn/joblib |
| API | Lambda Function URL |

---

## Repository Structure

```text
.
├── app.py                         # ETA 계산 Lambda handler
├── query_eta_app.py               # ETA 조회 GET API Lambda handler
├── model_loader.py                # S3 모델 artifact 로더
├── predict.py                     # 작업시간 예측 로직
├── clients/
│   ├── osrm_client.py             # OSRM route API client
│   └── route_result_client.py     # 경로최적화 결과 조회 client
├── queries/
│   ├── itemdata.py                # 배송 아이템 조회 샘플 쿼리
│   └── apartment_flag.py          # 주소/건물 속성 조회 샘플 쿼리
├── utils/
│   ├── db_handler.py
│   ├── db_handler_pg.py
│   ├── event_parser.py
│   └── sector_utils.py
├── events/
│   ├── event.json
│   └── shipping_complete_event.json
├── docs/images/
│   ├── architecture.png
│   ├── tms_eta_1.png
│   └── tms_eta_2.png
├── template.yaml
├── samconfig.example.toml
├── Dockerfile
└── requirements.txt
```

---

## Local Test

```bash
sam build --no-cached
sam local invoke TasuEtaCalculateFunction -e events/event.json
```

조회 Lambda 테스트:

```bash
sam local invoke TasuEtaQueryFunction -e events/query_event.json
```

---

## Deploy Example

실제 계정 정보는 `samconfig.toml`에 직접 커밋하지 않고, 아래 예시 파일을 복사해서 로컬에서만 사용합니다.

```bash
cp samconfig.example.toml samconfig.toml
sam build --no-cached --config-env dev
sam deploy --config-env dev
```

---

## Security / Redaction

공개 저장소 업로드 전 다음 항목을 제거하거나 샘플 값으로 대체했습니다.

- 실제 AWS 계정 ID
- 실제 S3 Bucket 이름
- 실제 Lambda Function URL
- 실제 EventBus 이름
- 실제 DB 테이블명 및 고객사명
- 실제 송장번호, 연락처, 사용자 ID
- `.git`, `__pycache__`, `.DS_Store`, `samconfig.toml`

---

## Key Takeaway

> 경로최적화 결과와 ML 기반 작업시간 예측 모델을 연결하여 TMS에서 조회 가능한 ETA 시스템을 구축했고,  
> EventBridge + Lambda + DynamoDB 기반으로 배송 완료 이벤트 이후 남은 배송지 ETA를 자동 갱신하는 구조를 만들었습니다.

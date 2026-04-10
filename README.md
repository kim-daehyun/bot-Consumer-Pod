# Bot Consumer Pod

Kafka 로그를 소비해 FE/BE 행동 데이터를 가공하고, 추론 서버에 전달한 뒤, Redis 기반 후속 조치까지 수행하는 소비자 서비스입니다.

발표 관점에서는 이 프로젝트를 다음 한 줄로 소개할 수 있습니다.

> "실시간 로그를 받아 사용자 행동을 특징값으로 변환하고, 봇 탐지 모델 결과를 정책 액션으로 연결하는 Kafka Consumer"

## 1. 프로젝트 목적

이 서비스는 세 종류의 로그를 받아 봇 의심 행위를 탐지하는 파이프라인의 중간 허브 역할을 합니다.

- FE 로그(`client_telemetry_log`)
  사용자의 마우스 이동, 페이지 체류 시간 등 프론트 행동 로그를 수집합니다.
- BE 요청 로그(`server_request_log`)
  로그인부터 결제까지 이어지는 서버 요청 흐름을 추적합니다.
- BE 이벤트 로그(`domain_event_log`)
  결제 성공/실패 같은 도메인 이벤트를 받아 세션을 마무리합니다.

최종적으로 이 서비스는:

1. 로그를 읽고
2. 세션 단위로 상태를 누적한 뒤
3. 모델 입력용 feature payload를 만들고
4. 추론 서버에 전달하고
5. 결과에 따라 Redis에 차단/위험 정보를 기록합니다.

## 2. 전체 처리 흐름

```text
Kafka Topic
  ├─ client_telemetry_log
  ├─ server_request_log
  └─ domain_event_log
        ↓
    ConsumerApp
        ↓
  FEProcessor / BEProcessor
        ↓
     StateStore(Redis + in-memory)
        ↓
    InferenceClient
        ↓
  /predict/fe 또는 /predict/be
        ↓
    ResultHandler
        ↓
  차단 티켓 저장 / 위험 사용자 집계 / 주문 위험 정보 저장
```

## 3. 핵심 구성 요소

### `main.py`

애플리케이션의 진입점입니다.

- Redis 연결 확인
- Inference 서버 health check 수행
- Kafka 모드 / 파일 테스트 모드 분기
- 토픽별로 FE/BE 처리기 호출
- 추론 결과를 후처리기로 전달

### `kafka_consumer.py`

Kafka에서 JSON 메시지를 읽어 `(topic, payload)` 형태로 넘겨주는 얇은 래퍼입니다.

### `fe_processor.py`

FE 로그를 세션 단위로 누적해 모델 입력 feature를 생성합니다.

주요 feature:

- `duration_ms`
- `mousemove_teleport_count`
- `mousemove_count`

핵심 아이디어:

- `UUID`, `sessionId` 등으로 세션 식별
- Redis에 FE 상태 누적
- `page_leave_ts`가 들어오면 종료 이벤트로 판단
- 마우스 좌표 이동 속도/거리 기반으로 비정상 이동 횟수 계산

### `be_processor.py`

BE 요청 로그와 이벤트 로그를 조합해 결제 흐름 기반 feature를 생성합니다.

주요 feature:

- `ts_payment_ready`
- `ts_whole_session`
- `req_interval_cv_pre_hold`
- `req_interval_cv_hold_gap`

핵심 아이디어:

- `orderId`, `reservationNumber`, `session key` 중 하나로 join key 생성
- 로그인부터 결제 완료까지 요청 흐름을 추적
- 좌석 선점 전후 요청 간격의 변동성을 계산
- 요청 로그와 도메인 이벤트가 모두 모여야 payload 생성

### `state_store.py`

상태 관리 전담 계층입니다.

- FE 상태: Redis에 저장
- FE 차단 티켓: Redis TTL 기반 저장
- BE 위험 사용자 카운트: Redis 증가 연산 사용
- BE 주문 위험 정보: Redis 저장
- BE join 상태: 프로세스 메모리에서 임시 관리

### `inference_client.py`

추론 서버와 HTTP 통신합니다.

- FE: `POST /predict/fe`
- BE: `POST /predict/be`

### `result_handler.py`

추론 결과를 실제 정책 액션으로 바꾸는 계층입니다.

FE 결과 처리:

- `label == bot` 이면 `X-Session-Ticket` 차단
- 차단 정보는 Redis에 TTL과 함께 저장

BE 결과 처리:

- `label == bot` 이면 주문 위험 정보 저장
- 사용자별 bot 탐지 횟수 누적
- 횟수에 따라 정책 액션 단계적 상향
- 선택적으로 외부 Risk User API 호출 가능

## 4. 토픽별 처리 방식

### FE 토픽: `client_telemetry_log`

입력 예시 성격:

- page 진입/이탈 시각
- viewport 크기
- mousemove 배열
- `X-Session-Ticket`
- `showScheduleId`

처리 결과:

- 좌석 선택 페이지 체류 시간 계산
- mousemove 이상 패턴 계산
- FE 모델 추론 요청
- 봇 판정 시 세션 티켓 차단

### BE 토픽: `server_request_log`

입력 예시 성격:

- 로그인 요청
- 좌석 선점 요청
- 결제 준비 요청
- 결제 성공/실패 요청

처리 결과:

- 세션 전 구간 시간 계산
- 요청 간격의 패턴성 계산
- BE 모델 추론 요청

### BE 토픽: `domain_event_log`

입력 예시 성격:

- 결제 성공 이벤트
- 결제 실패 이벤트
- 승인 시각 정보

처리 결과:

- `server_request_log`에서 만든 중간 상태를 마무리
- 최종 terminal 시점을 채워 feature 완성

## 5. 정책 액션 로직

BE 모델이 `bot`으로 판정한 경우 사용자 위험도는 누적 횟수에 따라 단계적으로 올라갑니다.

- 1회: `store_and_review`
- 2회: `raise_risk_and_monitor`
- 3회: `extra_auth_or_stricter_threshold`
- 4~5회: `temporary_restriction`
- 6회 이상: `consider_permanent_ban`

즉, 단발성 탐지에 바로 강한 제재를 거는 구조가 아니라, 누적 위험도를 기반으로 대응 강도를 높이는 방식입니다.

## 6. 실행 모드

이 프로젝트는 두 가지 모드를 지원합니다.

### 1. Kafka 모드

실서비스/통합 테스트용 모드입니다.

```bash
python main.py
```

기본값:

- `RUN_MODE=kafka`

### 2. File 모드

샘플 JSON 파일로 단건 테스트할 때 사용합니다.

```bash
RUN_MODE=file python main.py client_telemetry_log sample_fe.json
RUN_MODE=file python main.py server_request_log sample_req.json
RUN_MODE=file python main.py domain_event_log sample_evt.json
```

## 7. 환경 변수

| 변수명 | 기본값 | 설명 |
|---|---|---|
| `RUN_MODE` | `kafka` | 실행 모드 (`kafka` / `file`) |
| `LOG_LEVEL` | `INFO` | 로그 레벨 |
| `INFERENCE_BASE_URL` | `http://localhost:8000` | 추론 서버 주소 |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis DB index |
| `TOPIC_FE` | `client_telemetry_log` | FE 로그 토픽 |
| `TOPIC_REQ` | `server_request_log` | BE 요청 로그 토픽 |
| `TOPIC_EVT` | `domain_event_log` | BE 이벤트 로그 토픽 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_GROUP_ID` | `bot-consumer-local` | Consumer group id |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | 오프셋 시작 위치 |
| `RISK_USER_API_URL` | `""` | 위험 사용자 외부 연동 API |
| `RISK_USER_API_TIMEOUT_SEC` | `3.0` | 외부 API timeout |

## 8. 설치 및 실행

### 로컬 실행

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

### Docker 실행

```bash
docker build -t bot-consumer-pod .
docker run --rm \
  -e RUN_MODE=kafka \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e REDIS_HOST=host.docker.internal \
  -e INFERENCE_BASE_URL=http://host.docker.internal:8000 \
  bot-consumer-pod
```

## 9. 발표용 핵심 포인트

발표 때는 아래 메시지 중심으로 설명하면 깔끔합니다.

### 한 줄 요약

"로그를 실시간으로 받아 모델 입력으로 바꾸고, 봇 탐지 결과를 정책으로 연결하는 소비자 서비스"

### 기술적 강점

- Kafka 기반 비동기 로그 소비
- Redis를 활용한 세션 상태 및 정책 상태 관리
- FE/BE 로그를 분리 처리해 모델별 feature 생성
- 추론 결과를 단순 출력이 아니라 실제 차단 정책으로 연결

### 설계 포인트

- FE는 Redis 기반 누적 상태 관리
- BE는 요청 로그 + 이벤트 로그 결합이 핵심
- 결과 처리 단계에서 정책 고도화 가능
- 외부 Risk API 연동까지 고려된 확장 구조

## 10. 파일 구조

```text
bot_consumer_pod/
├── main.py
├── kafka_consumer.py
├── fe_processor.py
├── be_processor.py
├── state_store.py
├── inference_client.py
├── result_handler.py
├── requirements.txt
└── Dockerfile
```

## 11. 정리

이 프로젝트는 단순 Kafka Consumer가 아니라,

- 로그 수집
- 상태 누적
- feature engineering
- 모델 연동
- 정책 실행

까지 이어지는 실시간 탐지 파이프라인의 실행 컴포넌트입니다.

발표에서는 "로그를 읽는 프로그램"보다 "탐지 결과를 실제 운영 정책으로 연결하는 중간 제어 계층"이라는 점을 강조하면 전달력이 좋습니다.

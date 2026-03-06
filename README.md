# 구름닷컴(분산 운영 환경 기반 가상 이커머스 서비스)
## 개요
- Kafka(MSK) 기반 EDA 환경에서 SAGA 보상 트랜잭션 + DLQ를 설계/구현해 주문 정합성과 복원력을 확보한 Order-Service 개발
- **해당 프로젝트에서 도메인 기준으로 분리되어 있는 MSA 서비스에서 주로 참여한 부분을 발췌함**
### 일정
- 2025.10 ~ 2025.12
### 맡은 역할
- Order-Delivery-Cart 도메인을 통합한 Order-Service 서비스 구현
- Kafka 기반 EDA에서 SAGA 패턴을 적용해 보상 트랜잭션을 설계·구현하고, 최종적 일관성을 확보하여 데이터 정합성 유지
- AWS MSK Kafka 환경에 맞춘 Topic/Partition/Consumer Group 설계 경험
# 사용 기술 및 개발 환경
- Language: Java 17
- Framework: Spring Boot, Spring Data JPA
- Database: PostgreSQL (prod), H2 (local)
- Messaging: Kafka (AWS MSK)
- Infra/DevOps: Docker
- Tools: Git, Jira, Notion, Slack
<!--
# 내용
## 전체 서비스 인프라
<img width="2981" height="2250" alt="프로펙트 클라우드 1팀  아키텍처 3차 drawio" src="https://github.com/user-attachments/assets/09f017bc-d09d-4840-8dec-2c7b5bbe00d2" />

## ERD
<img width="2338" height="2901" alt="프로펙트 클라우드 1팀  ERD-Page-1 drawio" src="https://github.com/user-attachments/assets/d25e3659-70a1-4ccd-b8af-42699293802f" />

## 트랜잭션 시퀀스
![diagram-sequence](https://github.com/user-attachments/assets/2d727c20-5c17-4021-a3f5-91265e83d112)

## 보상 트랜잭션
- 주문 생성 이후 결제 실패/재고 부족/배송 생성 실패 등의 상황을 고려해 보상 트랜잭션을 설계
- 실패 이벤트 수신 시 주문 상태를 변경하고, 이전 단계 작업을 rollback 하도록 이벤트를 발행
- 각 서비스는 보상 이벤트를 수신하여 자신의 로컬 트랜잭션을 되돌림

<img width="2114" height="683" alt="프로펙트 클라우드 1팀  토픽-이벤트 설계 drawio (4)" src="https://github.com/user-attachments/assets/9689045f-6482-4b49-86f4-4e3d5fd3b5bb" />

## DLQ
- 재시도 후에도 처리 불가능한 이벤트는 DLQ로 격리하여 메인 컨슈머 지연을 방지
- DLQ를 기반으로 원인 분석 및 재처리 전략을 운영 가능하게 설계

<img width="1753" height="762" alt="프로펙트 클라우드 1팀  토픽-이벤트 설계 drawio (2)" src="https://github.com/user-attachments/assets/b04d20b0-ac58-4a8f-bb5e-fe7662bf29d0" />

## 파티셔닝 전략
- Producer
- 키 기반 해시 전략, Partition Key = OrderId
  - 동일 주문(OrderId)에 대한 이벤트는 동일 파티션으로 라우팅되도록 구성하여 주문 단위 처리 순서를 보장
  <img width="1047" height="547" alt="프로젝트 최종 발표" src="https://github.com/user-attachments/assets/0ef6480e-e815-4473-83a5-078dff3abaa4" />

- Consumer
- Cooperative Sticky Assignor 전략
  - 처리 중인 주문 이벤트가 다른 Consumer로 갑자기 이동하는 상황을 줄여 중복 처리/보상 트랜잭션 오작동 가능성을 낮춤
<img width="1117" height="590" alt="프로젝트 최종 발표 (1)" src="https://github.com/user-attachments/assets/f6e33dcd-6c67-4a84-b35c-fb5a88e8ed4c" />

## Kafka 토픽 설계
### 토픽 예시(Order-Service)
| Topic | Producer | Consumer Group | EventType | Key | 목적 |
|------|----------|----------------|----------|-----|------|
| `order-service-topic` | Delivery-Service | Order-Service | `DELIVERY_CREATED_SUCCESS` | `orderId` | 주문 상태 갱신 |
| `order-service-topic` | Delivery-Service | Order-Service | `DELIVERY_CREATED_FAIL` | `orderId` | 보상 트랜잭션 트리거 |
| `order-service-topic` | Stock-Service | Order-Service | `STOCK_RESTORE_FAIL` | `orderId` | 장애 감지 및 후속 처리 |
- 서비스 중심 토픽 설계  
   - 프로듀서 파티셔닝 전략과 맞춰, 각 서비스별 도메인 특성에 맞게 독립적인 파티션 키 할당하여 관리
<img width="741" height="261" alt="프로펙트 클라우드 1팀  토픽-이벤트 설계 drawio (1)" src="https://github.com/user-attachments/assets/64b86445-dc09-490b-b9be-a3bd2380862a" />

# 트러블 슈팅
- 문제 상황 : OrderID로 이벤트의 순서는 보장했지만 이벤트 타입 구분이 없어 특정 서비스가 자신과 무관한 이벤트를 소비하는 문제가 발생
  - 예시 : 배달 시작 이벤트를 재고 관련 Consumer Group이 소비
- 영향 : 배송 시작 이벤트를 재고 Consumer가 처리하여 불필요한 로직 실행/예외 발생 가능
- 원인 : 이벤트 설계 과정에서 EventType의 부재
- 해결 : EventEnvelope 객체를 도입해 도메인 이벤트 객체를 Envelope으로 감싸고, 이벤트 객체를 직렬화한 값을 payload에 저장하는 방식으로 이벤트 설계
<img width="720" height="26" alt="image (2)" src="https://github.com/user-attachments/assets/a1ea7c1f-758c-4bd4-a3c8-d6b7d24d80c9" />
<img width="480" height="232" alt="image (1)" src="https://github.com/user-attachments/assets/d2fa54db-bcce-4183-87cc-8c22e4d8f746" />
-->
### 관련 코드
- Kafka 관련
  - `src/main/kafka`
- Order 관련
  - `src/main/order`
  

# 구름닷컴(분산 운영 환경 기반 가상 이커머스 서비스)
## 개요
- Spring Boot를 활용한 MSA 이커머스 서비스
- AWS MSK 기반 Kafka 운영
- **해당 프로젝트에서 도메인 기준으로 분리되어 있는 MSA 서비스에서 주로 참여한 부분을 발췌함**
### 일정
- 2025.10 ~ 2025.12
### 맡은 역할
- Order-Delivery-Cart 도메인을 통합한 Order-Service 서비스 구현
- Kafka 기반 EDA에서 SAGA 패턴을 적용해 보상 트랜잭션을 설계·구현하고, 최종적 일관성을 확보하여 데이터 정합성 유지
- 파티션 전략 설계 및 MSK 클러스터 구성
# 사용 기술 및 개발 환경
- DB : Postgres(운영), H2(개발)
- Framwork/Flatform : Spring boot, Docker, Kafka
- Infrastructure : AWS MSK
- Language : Java
- Tool : Slack, Git, Jira, Notion
# 내용
## 전체 서비스 인프라
<img width="2981" height="2250" alt="프로펙트 클라우드 1팀  아키텍처 3차 drawio" src="https://github.com/user-attachments/assets/09f017bc-d09d-4840-8dec-2c7b5bbe00d2" />

## ERD
<img width="2338" height="2901" alt="프로펙트 클라우드 1팀  ERD-Page-1 drawio" src="https://github.com/user-attachments/assets/d25e3659-70a1-4ccd-b8af-42699293802f" />

## 트랜잭션 시퀀스
![diagram-sequence](https://github.com/user-attachments/assets/2d727c20-5c17-4021-a3f5-91265e83d112)

## 보상 트랜잭션
<img width="2114" height="683" alt="프로펙트 클라우드 1팀  토픽-이벤트 설계 drawio (4)" src="https://github.com/user-attachments/assets/9689045f-6482-4b49-86f4-4e3d5fd3b5bb" />

## DLQ
<img width="1753" height="762" alt="프로펙트 클라우드 1팀  토픽-이벤트 설계 drawio (2)" src="https://github.com/user-attachments/assets/b04d20b0-ac58-4a8f-bb5e-fe7662bf29d0" />

## 파티셔닝 전략
- Producer
- 키 기반 해시 전략
  - 같은 이벤트는 동일한 파티션으로 보내서 순서를 보장
  - 주문 관련 트랜잭션은 순서가 보장되어야 올바르게 지정 되어야 같은 주문에 대해 배송, 결제, 재고 차감 등 의 과정이 올바르게 수행
  <img width="1047" height="547" alt="프로젝트 최종 발표" src="https://github.com/user-attachments/assets/0ef6480e-e815-4473-83a5-078dff3abaa4" />

- Consumer
- 협력적 스티키 전략
  - OrderId가 동일한 컨슈머에서 병렬로 처리되는 것을 방지해 중복 보상/처리 가능성을 방지
<img width="1117" height="590" alt="프로젝트 최종 발표 (1)" src="https://github.com/user-attachments/assets/f6e33dcd-6c67-4a84-b35c-fb5a88e8ed4c" />

## Kafka 토픽 설계
- 서비스 중심 토픽 설계  
   - 프로듀서 파티셔닝 전략과 맞춰, 각 서비스별 도메인 특성에 맞게 독립적인 파티션 키 할당하여 관리
<img width="741" height="261" alt="프로펙트 클라우드 1팀  토픽-이벤트 설계 drawio (1)" src="https://github.com/user-attachments/assets/64b86445-dc09-490b-b9be-a3bd2380862a" />

# 트러블 슈팅
- 문제 상황 : OrderID로 이벤트의 순서는 보장했지만 이벤트 종류 판단 어려움
  - 예시 : 배달 시작 이벤트를 재고 관련 컨슈머 그룹이 소비
- 원인 : 이벤트 설계 과정에서 EventType의 부재
- 해결 : EventEnvelope 객체를 도입해 도메인 이벤트 객체를 Envelope으로 감싸고, 이벤트 객체를 직렬화한 값을 payload에 저장하는 방식으로 이벤트 설계
<img width="720" height="26" alt="image (2)" src="https://github.com/user-attachments/assets/a1ea7c1f-758c-4bd4-a3c8-d6b7d24d80c9" />
<img width="480" height="232" alt="image (1)" src="https://github.com/user-attachments/assets/d2fa54db-bcce-4183-87cc-8c22e4d8f746" />

  

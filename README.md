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
# 내용
## 전체 서비스 인프라
<img width="2981" height="2250" alt="프로펙트 클라우드 1팀  아키텍처 3차 drawio" src="https://github.com/user-attachments/assets/09f017bc-d09d-4840-8dec-2c7b5bbe00d2" />

## ERD
<img width="2338" height="2901" alt="프로펙트 클라우드 1팀  ERD-Page-1 drawio" src="https://github.com/user-attachments/assets/d25e3659-70a1-4ccd-b8af-42699293802f" />

## 트랜잭션 시퀀스
![diagram-sequence](https://github.com/user-attachments/assets/2d727c20-5c17-4021-a3f5-91265e83d112)
 

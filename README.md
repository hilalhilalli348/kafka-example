> Language / Dil: [AZ](#az) | [EN](#en)

---

## AZ

# Kafka Outbox Pattern Demo

Bu layihə, mikroservis arxitekturasında məlumatın etibarlı şəkildə ötürülməsi üçün istifadə olunan **Outbox Pattern**-in sadə tətbiqidir. PostgreSQL, Debezium və Kafka vasitəsilə məlumatın itmədən ötürülməsini təmin edir.

## Necə İşlətməli?

Layihəni işə salmaq üçün ardıcıl olaraq aşağıdakı addımları izləyin:

### 1. İnfrastrukturu başladın (Docker)
Bütün lazımi servisləri (Kafka, PostgreSQL, Debezium, Redpanda Console) ayağa qaldırın:
```bash
docker-compose up -d
```

### 2. Debezium Connector-u qeydiyyatdan keçirin
PostgreSQL-dəki `outbox` cədvəlini izləmək üçün connectoru aktivləşdirin:
```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium-source-connector.json
```

### 3. Tətbiqi işə salın
Spring Boot layihəsini başladın:
```bash
./gradlew bootRun
```

### 4. Test edin
Yeni istifadəçi yaratmaq üçün sorğu göndərin (bu zaman həm DB-yə yazılacaq, həm də Outbox vasitəsilə Kafka-ya mesaj gedəcək):
```bash
curl -X POST http://localhost:8085/users \
     -H "Content-Type: application/json" \
     -d '{"username": "johndoe"}'
```

## Faydalı Linklər
- **Kafka UI (Redpanda Console):** [http://localhost:8081](http://localhost:8081) — Mesajları buradan izləyə bilərsiniz.
- **Debezium Status:** `curl localhost:8083/connectors/outbox-connector/status`

---

## EN

# Kafka Outbox Pattern Demo

This project is a simple implementation of the **Outbox Pattern**, which is used to reliably transfer data between microservices. It uses PostgreSQL, Debezium, and Kafka to make sure no messages are lost during transmission.

## How to Run

Follow these steps in order to get the project up and running:

### 1. Start the Infrastructure (Docker)
Bring up all required services (Kafka, PostgreSQL, Debezium, Redpanda Console):
```bash
docker-compose up -d
```

### 2. Register the Debezium Connector
Activate the connector to watch the `outbox` table in PostgreSQL:
```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium-source-connector.json
```

### 3. Start the Application
Run the Spring Boot project:
```bash
./gradlew bootRun
```

### 4. Test It
Send a request to create a new user. This will write to the database and send a message to Kafka through the Outbox:
```bash
curl -X POST http://localhost:8085/users \
     -H "Content-Type: application/json" \
     -d '{"username": "johndoe"}'
```

## Useful Links
- **Kafka UI (Redpanda Console):** [http://localhost:8081](http://localhost:8081) — View and monitor messages here.
- **Debezium Status:** `curl localhost:8083/connectors/outbox-connector/status`

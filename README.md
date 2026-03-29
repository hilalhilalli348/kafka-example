> Language / Dil: [AZ](#az) | [EN](#en)

## AZ

# Kafka Schema Registry Demo

Bu layihə Apache Kafka, Avro və Confluent Schema Registry inteqrasiyasını göstərən sadə Spring Boot nümunəsidir.

Burada əsas fikir budur:

- Producer `user-created-event-topic` topic-ə Avro formatında `UserCreatedEvent` göndərir.
- Mesaj göndərilən anda schema Schema Registry-də saxlanılır və ya mövcud schema ilə yoxlanılır.
- Consumer mesajı `KafkaAvroDeserializer` ilə oxuyur və birbaşa generated Avro class-a çevirir.
- Əgər mesaj korlanıbsa və ya Avro formatına uyğun deyilsə, `DefaultErrorHandler` onu idarə edir.
- Poison pill tipli mesajlar `user-created-event-topic-dlt` topic-inə yönləndirilə bilər.

## Layihədə nə göstərilir

Bu repo aşağıdakı davranışları göstərmək üçün hazırlanıb:

- Spring Kafka producer tərəfdə `KafkaAvroSerializer` istifadə edir.
- Consumer tərəfdə `KafkaAvroDeserializer` və `specific.avro.reader=true` ilə specific Avro record oxunur.
- Avro schema `src/main/avro/UserCreatedEvent.avsc` faylından generasiya olunur.
- Schema Registry URL tətbiq konfiqurasiyasında ayrıca verilir.
- 3 broker-li Kafka cluster, ayrıca Schema Registry və Schema Registry UI docker-compose ilə qaldırılır.
- `DefaultErrorHandler` və DLT konfiqurasiyası korlanmış və ya deserialize olunmayan mesajların consumer-i bloklamamasını göstərir.

## Əsas fayllar

- [src/main/avro/UserCreatedEvent.avsc](/Users/hilalhilalli/Desktop/kafka-example/src/main/avro/UserCreatedEvent.avsc)
- [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml)
- [src/main/java/ourcorp/kafka/example/config/KafkaConfig.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/config/KafkaConfig.java)
- [src/main/java/ourcorp/kafka/example/producer/UserProducer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/producer/UserProducer.java)
- [src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java)
- [docker-compose.yml](/Users/hilalhilalli/Desktop/kafka-example/docker-compose.yml)

## Texniki axın

### 1. Avro schema

`UserCreatedEvent` schema-sı `src/main/avro/UserCreatedEvent.avsc` daxilində saxlanılır.

- Schema bir `record` tipidir.
- Namespace: `ourcorp.kafka.example.model.event`
- Field: `name`

Gradle-də Avro plugin istifadə olunduğu üçün build zamanı bu schema-dan Java class generasiya olunur.

### 2. Producer

`UserProducer` REST request-dən gələn datanı `UserCreatedEvent` obyektinə çevirib `user-created-event-topic` topic-ə göndərir.

- Key olaraq random UUID yazılır.
- `X-USER-ID` custom header əlavə olunur.
- Value serializer olaraq `io.confluent.kafka.serializers.KafkaAvroSerializer` istifadə edilir.

Bu serializer payload-u Avro formatında Kafka-ya yazır və schema-nı Schema Registry ilə əlaqələndirir.

### 3. Schema Registry

Schema Registry Kafka mesajlarının schema idarəsini mərkəzləşdirir.

Bu layihədə:

- Schema Registry `http://localhost:8082` ünvanında işləyir.
- Producer mesaj göndərərkən schema-nı registry-ə qeyd edir və ya uyğunluğunu yoxlayır.
- Consumer həmin schema metadata əsasında payload-u düzgün record tipinə çevirir.

Bu yanaşma producer və consumer arasında data kontraktını daha təhlükəsiz saxlayır.

### 4. Consumer

Consumer konfiqurasiyasında:

- `ErrorHandlingDeserializer`
- delegate kimi `KafkaAvroDeserializer`
- `specific.avro.reader=true`

istifadə olunur.

Nəticə olaraq consumer mesajı generic obyekt kimi deyil, birbaşa `UserCreatedEvent` kimi qəbul edir.

### 5. Error handling və DLT

`KafkaConfig` daxilində:

- `DefaultErrorHandler`
- `DeadLetterPublishingRecoverer`
- `FixedBackOff(500ms, 2)`

istifadə olunur.

Əgər consumer Avro mesajını deserialize edə bilmirsə, məsələn "unknown magic byte" kimi problem yaranırsa:

- exception retry olunmur
- mesaj consumer axınını bloklamır
- record `user-created-event-topic-dlt` topic-inə yönləndirilir

DLT üçün ayrıca `ByteArraySerializer` istifadə olunur ki, korlanmış payload-u raw şəkildə publish etmək mümkün olsun.

## Necə işə salmaq olar

### Tələblər

- Java 21
- Docker / Docker Compose

### 1. Kafka və Schema Registry mühitini başladın

```bash
docker-compose up -d
```

Bu compose aşağıdakı servisləri qaldırır:

- `kafka1`
- `kafka2`
- `kafka3`
- `schema-registry`
- `schema-registry-ui`
- `redpanda-console`

### 2. Tətbiqi başladın

```bash
./gradlew bootRun
```

Application default olaraq `8080` portunda açılır.

### 3. Test mesajı göndərin

```bash
curl -X POST http://localhost:8080/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"name":"Hilal"}'
```

## Nəyə baxmaq lazımdır

- Producer `user-created-event-topic` topic-ə Avro event göndərəcək.
- Consumer mesajı `UserCreatedEvent` kimi oxuyub log-a yazacaq.
- Schema Registry daxilində bu event üçün schema qeyd olunacaq.
- Redpanda Console üzərindən topic və message-ləri izləmək olar: `http://localhost:8081`
- Schema Registry UI üzərindən subject və schema-lara baxmaq olar: `http://localhost:8000`

## Schema Registry ssenariləri

Bu repo Schema Registry istifadəsinin əsas davranışlarını göstərmək üçün uyğundur.

### 1. Normal Avro publish/consume

Əgər producer düzgün `UserCreatedEvent` göndərirsə:

- schema registry-də subject yaranır
- mesaj topic-ə Avro binary formatında yazılır
- consumer onu `UserCreatedEvent` kimi uğurla oxuyur

### 2. Schema əsaslı kontrakt idarəsi

Avro schema payload strukturunu əvvəlcədən müəyyənləşdirir.

Bu o deməkdir ki:

- producer sərbəst JSON deyil, schema ilə məhdudlaşmış event göndərir
- consumer də eyni kontrakta əsasən deserialize edir
- event formatı daha idarəolunan olur

Bu yanaşma xüsusilə servis-lərarası event müqaviləsi üçün faydalıdır.

### 3. Poison pill / deserialize problemi

Əgər topic-ə Avro olmayan və ya korlanmış mesaj düşərsə:

- `ErrorHandlingDeserializer` xətanı tutur
- `DefaultErrorHandler` problemi emal edir
- deserialize xətası retry edilmədən DLT-yə yönləndirilə bilər

Bu, consumer qrupunun bir dənə pis mesaj üzündən dayanmasının qarşısını alır.

## Vacib qeyd

Hazırkı `UserConsumer` sadəcə event-i log edir. Bu repo-nun əsas fokus nöqtəsi biznes məntiqindən çox Schema Registry inteqrasiyası, specific Avro deserialization və problemli mesajların idarəsidir.

## Faydalı ünvanlar

- App: `http://localhost:8080`
- Schema Registry: `http://localhost:8082`
- Schema Registry UI: `http://localhost:8000`
- Redpanda Console: `http://localhost:8081`
- Main topic: `user-created-event-topic`
- DLT topic: `user-created-event-topic-dlt`

---

## EN

# Kafka Schema Registry Demo

This project is a simple Spring Boot example that demonstrates Apache Kafka, Avro, and Confluent Schema Registry integration.

The main flow is:

- The producer sends an Avro `UserCreatedEvent` to `user-created-event-topic`.
- While sending, the schema is registered in Schema Registry or validated against an existing one.
- The consumer reads the message with `KafkaAvroDeserializer` and converts it into a generated Avro class.
- If the message is corrupted or not in valid Avro format, `DefaultErrorHandler` handles the failure.
- Poison pill records can be routed to `user-created-event-topic-dlt`.

## What this project demonstrates

- Spring Kafka producer uses `KafkaAvroSerializer`.
- Consumer uses `KafkaAvroDeserializer` with `specific.avro.reader=true` to read a specific Avro record.
- The Avro schema is defined in `src/main/avro/UserCreatedEvent.avsc`.
- Schema Registry is configured explicitly in application properties.
- A 3-broker Kafka cluster, Schema Registry, and Schema Registry UI are started via docker-compose.
- `DefaultErrorHandler` and DLT configuration prevent broken messages from blocking consumption.

## Main files

- [src/main/avro/UserCreatedEvent.avsc](/Users/hilalhilalli/Desktop/kafka-example/src/main/avro/UserCreatedEvent.avsc)
- [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml)
- [src/main/java/ourcorp/kafka/example/config/KafkaConfig.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/config/KafkaConfig.java)
- [src/main/java/ourcorp/kafka/example/producer/UserProducer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/producer/UserProducer.java)
- [src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java)
- [docker-compose.yml](/Users/hilalhilalli/Desktop/kafka-example/docker-compose.yml)

## Technical flow

### 1. Avro schema

The `UserCreatedEvent` schema is defined in `src/main/avro/UserCreatedEvent.avsc`.

- It is an Avro `record`
- Namespace: `ourcorp.kafka.example.model.event`
- Field: `name`

The Gradle Avro plugin generates the Java class from this schema during build time.

### 2. Producer

`UserProducer` converts the REST payload into a `UserCreatedEvent` and sends it to `user-created-event-topic`.

- A random UUID is used as the key.
- A custom `X-USER-ID` header is added.
- `io.confluent.kafka.serializers.KafkaAvroSerializer` is used as the value serializer.

This serializer writes the payload in Avro format and associates the schema with Schema Registry.

### 3. Schema Registry

Schema Registry centralizes schema management for Kafka messages.

In this project:

- Schema Registry runs at `http://localhost:8082`
- The producer registers the schema or checks compatibility when publishing
- The consumer uses schema metadata to deserialize the payload into the correct record type

This makes the data contract between producer and consumer more reliable.

### 4. Consumer

The consumer is configured with:

- `ErrorHandlingDeserializer`
- delegated `KafkaAvroDeserializer`
- `specific.avro.reader=true`

As a result, the listener receives `UserCreatedEvent` directly instead of a generic object.

### 5. Error handling and DLT

Inside `KafkaConfig`, the project uses:

- `DefaultErrorHandler`
- `DeadLetterPublishingRecoverer`
- `FixedBackOff(500ms, 2)`

If the consumer cannot deserialize a record, for example due to an "unknown magic byte" problem:

- the exception is not retried
- consumption is not blocked
- the record can be published to `user-created-event-topic-dlt`

A separate DLT producer uses `ByteArraySerializer` so corrupted payloads can still be forwarded as raw bytes.

## How to run

### Requirements

- Java 21
- Docker / Docker Compose

### 1. Start Kafka and Schema Registry

```bash
docker-compose up -d
```

This compose file starts:

- `kafka1`
- `kafka2`
- `kafka3`
- `schema-registry`
- `schema-registry-ui`
- `redpanda-console`

### 2. Start the application

```bash
./gradlew bootRun
```

The application runs on port `8080`.

### 3. Send a test message

```bash
curl -X POST http://localhost:8080/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"name":"Hilal"}'
```

## What to check

- The producer sends an Avro event to `user-created-event-topic`
- The consumer logs the message as `UserCreatedEvent`
- A schema subject is created in Schema Registry
- You can inspect topics and messages in Redpanda Console: `http://localhost:8081`
- You can inspect subjects and schemas in Schema Registry UI: `http://localhost:8000`

## Schema Registry scenarios

This repository is a good example of the main Schema Registry behaviors.

### 1. Normal Avro publish/consume

If the producer sends a valid `UserCreatedEvent`:

- a subject is created in Schema Registry
- the message is written to Kafka in Avro binary format
- the consumer reads it successfully as `UserCreatedEvent`

### 2. Schema-based contract management

The Avro schema defines the payload structure up front.

That means:

- the producer does not send arbitrary JSON, but a schema-bound event
- the consumer deserializes against the same contract
- the event format becomes easier to govern

This is especially useful for service-to-service event contracts.

### 3. Poison pill / deserialization issue

If a non-Avro or corrupted record is written into the topic:

- `ErrorHandlingDeserializer` catches the failure
- `DefaultErrorHandler` processes the error
- the deserialization failure can be sent to DLT without retry

This prevents a single bad record from stopping the consumer group.

## Important note

The current `UserConsumer` only logs the event. The main focus of this repository is Schema Registry integration, specific Avro deserialization, and safe handling of problematic records rather than business logic.

## Useful endpoints

- App: `http://localhost:8080`
- Schema Registry: `http://localhost:8082`
- Schema Registry UI: `http://localhost:8000`
- Redpanda Console: `http://localhost:8081`
- Main topic: `user-created-event-topic`
- DLT topic: `user-created-event-topic-dlt`

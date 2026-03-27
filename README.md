> Language / Dil: [AZ](#az) | [EN](#en)

## AZ

# Kafka Error Handling Demo

Bu layihə Spring Kafka-da `ErrorHandlingDeserializer`, `DefaultErrorHandler` və `Dead Letter Topic (DLT)` axınının necə işlədiyini göstərən sadə nümunədir.

Burada əsas fikir budur:

- Producer `user-created-event-topic` topic-ə JSON event göndərir.
- Consumer mesajı `ErrorHandlingDeserializer` ilə deserialize edir.
- Deserialize və ya processing zamanı xəta baş verərsə, Spring Kafka `DefaultErrorHandler`-ı işə salır.
- Retry olunmalı xətalar bir neçə dəfə yenidən yoxlanılır.
- Retry olunmayan və ya retry limitini keçən mesajlar `user-created-event-topic-dlt` topic-inə yönləndirilir.

## Layihədə nə göstərilir

Bu repo aşağıdakı davranışları göstərmək üçün hazırlanıb:

- `ErrorHandlingDeserializer` deserialize xətasını listener-i tam qırmadan idarə edir.
- `JsonDeserializer` istifadə olunur və producer mesaj header-larında type məlumatı göndərir.
- `DefaultErrorHandler` `FixedBackOff(500ms, 3)` ilə retry edir.
- `RetryableException` retry edilən exception kimi qeyd olunub.
- `NotRetryableException` retry edilmədən reject olunan exception kimi qeyd olunub.
- `DeadLetterPublishingRecoverer` problemli mesajı eyni partition üzrə DLT-yə publish edir.

## Əsas fayllar

- [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml)
- [src/main/java/ourcorp/kafka/example/config/KafkaConfig.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/config/KafkaConfig.java)
- [src/main/java/ourcorp/kafka/example/producer/UserProducer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/producer/UserProducer.java)
- [src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java)

## Texniki axın

### 1. Producer

`UserProducer` `UserCreatedEvent` obyektini `user-created-event-topic` topic-ə göndərir.

- Key olaraq random UUID yazılır.
- `X-USER-ID` custom header əlavə olunur.
- `JsonSerializer` və `spring.json.add.type.headers=true` səbəbilə type header da yazılır.

Bu type header consumer tərəfində JSON-u hansı class-a çevirmək lazım olduğunu müəyyən edir.

### 2. Consumer deserialize mərhələsi

Consumer konfiqurasiyasında:

- `value-deserializer=ErrorHandlingDeserializer`
- delegate kimi `JsonDeserializer`

Bu o deməkdir ki, JSON parse və ya class-a çevirmə problemi olsa, exception birbaşa consumer thread-i partlatmır. Xəta Spring Kafka error handling mexanizminə ötürülür.

### 3. DefaultErrorHandler

`KafkaConfig` daxilində:

- `DefaultErrorHandler`
- `DeadLetterPublishingRecoverer`
- `FixedBackOff(500L, 3)`

istifadə olunur.

Davranış:

- `RetryableException` atılarsa, mesaj 3 dəfə 500 ms intervalla yenidən yoxlanılır.
- `NotRetryableException` atılarsa, retry olunmur və mesaj birbaşa DLT-yə göndərilir.
- Deserialize xətası kimi recover edilə bilməyən problemlər də DLT-yə düşə bilər.

### 4. DLT

Problemli mesajlar `user-created-event-topic-dlt` topic-inə yazılır.

Mapping belədir:

- əsas topic: `user-created-event-topic`
- DLT: `user-created-event-topic-dlt`
- partition qorunur

Bu mapping `DeadLetterPublishingRecoverer` ilə verilib.

## Necə işə salmaq olar

### Tələblər

- Java 21
- Docker / Docker Compose

### 1. Kafka mühitini başladın

```bash
docker-compose up -d
```

Bu compose aşağıdakı servisləri qaldırır:

- `kafka1`
- `kafka2`
- `kafka3`
- `redpanda-console`

### 2. Tətbiqi başladın

```bash
./gradlew bootRun
```

Application default olaraq `8085` portunda açılır.

### 3. Test mesajı göndərin

```bash
curl -X POST http://localhost:8085/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"name":"Hilal"}'
```

## Nəyə baxmaq lazımdır

- Producer mesajı `user-created-event-topic` topic-ə göndərəcək.
- Consumer normal halda event-i log-a yazacaq.
- Redpanda Console üzərindən topic və message-ləri izləmək olar: `http://localhost:8081`

## Error handling ssenariləri

Bu repo-da error handling konfiqurasiyası hazırdır. Aşağıdakı ssenarilər həmin konfiqurasiyanın nə etdiyini izah edir.

### 1. Deserialize xətası

Əgər topic-ə uyğun olmayan payload və ya uyğun olmayan type header ilə mesaj düşsə:

- `JsonDeserializer` çevirməni edə bilmir
- `ErrorHandlingDeserializer` xətanı tutur
- xəta listener container-a ötürülür
- `DefaultErrorHandler` recover etməyə çalışır
- nəticədə mesaj DLT-yə yönləndirilə bilər

Bu, xüsusilə producer type header göndərmədikdə və ya payload gözlənilən modelə uyğun olmadıqda faydalıdır.

### 2. Retryable exception

Əgər listener daxilində biznes xətası müvəqqətidirsə və `RetryableException` atılırsa:

- `DefaultErrorHandler` mesajı yenidən emal edir
- retry sayı: `3`
- backoff: `500 ms`
- yenə də uğursuz olarsa mesaj DLT-yə gedir

Bu ssenari müvəqqəti downstream problem, network timeout və s. üçün uyğundur.

### 3. Not retryable exception

Əgər xəta permanent-dirsə və `NotRetryableException` atılırsa:

- retry edilmir
- mesaj birbaşa DLT-yə göndərilir

Bu ssenari validation xətası, korlanmış data və ya biznes qaydasının pozulması üçün uyğundur.

## Vacib qeyd

Hazırkı `UserConsumer` sadəcə event-i log edir. Yəni retry və DLT davranışını real şəkildə görmək üçün listener daxilində test məqsədli exception atmaq və ya topic-ə deserialize olunmayan mesaj göndərmək lazımdır.

Məsələn, demo üçün `handleUserCreated(...)` daxilində müvəqqəti olaraq:

```java
throw new RetryableException("temporary problem");
```

və ya

```java
throw new NotRetryableException("bad payload");
```

kimi sınaq edilə bilər.

## Faydalı ünvanlar

- App: `http://localhost:8085`
- Redpanda Console: `http://localhost:8081`
- Main topic: `user-created-event-topic`
- DLT topic: `user-created-event-topic-dlt`

---

## EN

# Kafka Error Handling Demo

This project is a simple Spring Kafka example focused on `ErrorHandlingDeserializer`, `DefaultErrorHandler`, and Dead Letter Topic (DLT) behavior.

The main flow is:

- The producer sends JSON messages to `user-created-event-topic`.
- The consumer deserializes messages with `ErrorHandlingDeserializer`.
- If deserialization or processing fails, Spring Kafka delegates the failure to `DefaultErrorHandler`.
- Retryable failures are retried.
- Non-retryable or exhausted failures are published to `user-created-event-topic-dlt`.

## What this project demonstrates

- `ErrorHandlingDeserializer` wraps deserialization failures and lets Spring Kafka handle them safely.
- `JsonDeserializer` is used as the delegate deserializer.
- The producer sends type headers with `spring.json.add.type.headers=true`.
- `DefaultErrorHandler` retries with `FixedBackOff(500ms, 3)`.
- `RetryableException` is configured as retryable.
- `NotRetryableException` is configured as non-retryable.
- `DeadLetterPublishingRecoverer` publishes failed records to the DLT on the same partition.

## Main files

- [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml)
- [src/main/java/ourcorp/kafka/example/config/KafkaConfig.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/config/KafkaConfig.java)
- [src/main/java/ourcorp/kafka/example/producer/UserProducer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/producer/UserProducer.java)
- [src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserConsumer.java)

## Technical flow

### 1. Producer

`UserProducer` sends a `UserCreatedEvent` to `user-created-event-topic`.

- A random UUID is used as the key.
- A custom `X-USER-ID` header is added.
- Type headers are also added by the JSON serializer.

Those type headers help the consumer understand which Java class the payload should be converted to.

### 2. Consumer deserialization stage

The consumer is configured with:

- `ErrorHandlingDeserializer`
- delegated `JsonDeserializer`

This means deserialization problems are routed into Spring Kafka's error-handling flow instead of crashing message consumption directly.

### 3. DefaultErrorHandler

In `KafkaConfig`, the project defines:

- `DefaultErrorHandler`
- `DeadLetterPublishingRecoverer`
- `FixedBackOff(500L, 3)`

Behavior:

- `RetryableException` is retried 3 times with a 500 ms backoff.
- `NotRetryableException` is not retried and is sent directly to the DLT.
- Deserialization failures can also end up in the DLT if they cannot be recovered.

### 4. DLT

Failed records are published to `user-created-event-topic-dlt`.

- source topic: `user-created-event-topic`
- DLT: `user-created-event-topic-dlt`
- partition is preserved

## How to run

### Requirements

- Java 21
- Docker / Docker Compose

### 1. Start Kafka infrastructure

```bash
docker-compose up -d
```

This starts:

- `kafka1`
- `kafka2`
- `kafka3`
- `redpanda-console`

### 2. Run the application

```bash
./gradlew bootRun
```

The app runs on port `8085`.

### 3. Send a test request

```bash
curl -X POST http://localhost:8085/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"name":"Hilal"}'
```

## What to observe

- The producer writes to `user-created-event-topic`.
- The consumer logs the event in the normal flow.
- You can inspect topics and messages in Redpanda Console: `http://localhost:8081`

## Error handling scenarios

### 1. Deserialization failure

If a malformed payload or incompatible type header is sent to the topic:

- `JsonDeserializer` fails
- `ErrorHandlingDeserializer` captures the failure
- the error is passed to the container
- `DefaultErrorHandler` handles recovery
- the record may be published to the DLT

### 2. Retryable exception

If the listener throws `RetryableException`:

- Spring Kafka retries processing
- retry count: `3`
- backoff: `500 ms`
- if it still fails, the record is sent to the DLT

### 3. Non-retryable exception

If the listener throws `NotRetryableException`:

- no retry happens
- the record is sent directly to the DLT

## Important note

The current `UserConsumer` only logs the event. To observe retry and DLT behavior directly, you need to temporarily throw a test exception inside the listener or publish a message that cannot be deserialized.

Example:

```java
throw new RetryableException("temporary problem");
```

or:

```java
throw new NotRetryableException("bad payload");
```

## Useful endpoints

- App: `http://localhost:8085`
- Redpanda Console: `http://localhost:8081`
- Main topic: `user-created-event-topic`
- DLT topic: `user-created-event-topic-dlt`

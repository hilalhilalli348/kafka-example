> Language / Dil: [AZ](#az) | [EN](#en)

## AZ

# Kafka Non-Blocking Retry və DLT Demo

Bu layihə Spring Kafka-da `@RetryableTopic` istifadə edərək non-blocking retry və Dead Letter Topic (DLT) axınının necə işlədiyini göstərən sadə nümunədir.

Layihədə iki fərqli consumer axını göstərilir:

- `UserCreatedConsumer` retryable ssenarini göstərir
- `UsernameChangedConsumer` non-retryable ssenarini göstərir

Beləliklə eyni repo daxilində həm retry olunan, həm də birbaşa DLT-yə gedən xəta nümunəsini görmək mümkündür.

## Layihədə nə göstərilir

Bu repo aşağıdakı davranışları göstərmək üçün hazırlanıb:

- Manual topic konfiqurasiyası (`NewTopic`, `TopicBuilder`)
- Deklarativ retry konfiqurasiyası (`@RetryableTopic`)
- Programmatic / imperative retry konfiqurasiyası (`RetryTopicConfigurationBuilder`) üçün nümunə
- `@RetryableTopic` ilə non-blocking retry qurulması
- Retry üçün yalnız seçilmiş exception-ların (`RetryableException`) daxil edilməsi
- Non-retryable exception nümunəsi (`NotRetryableException`)
- Eksponential backoff davranışı: `200ms -> 600ms -> 1800ms`
- Retry topic və DLT üçün `numPartitions = 2`, `replicationFactor = 3`
- DLT mesajlarının `@DltHandler` ilə tutulması
- Producer tərəfində JSON type header göndərilməsi
- Consumer tərəfində `ErrorHandlingDeserializer` + `JsonDeserializer` istifadəsi

## Əsas fayllar

- [README.md](/Users/hilalhilalli/Desktop/kafka-example/README.md)
- [src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java)
- [src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java)
- [src/main/java/ourcorp/kafka/example/producer/UserProducer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/producer/UserProducer.java)
- [src/main/java/ourcorp/kafka/example/controller/UserController.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/controller/UserController.java)
- [src/main/java/ourcorp/kafka/example/config/KafkaConfig.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/config/KafkaConfig.java)
- [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml)

## İstifadə olunan yanaşmalar

Bu layihə bir neçə fərqli konfiqurasiya yanaşmasını eyni yerdə göstərmək məqsədi ilə hazırlanıb:

- Manual topic konfiqurasiyası: `KafkaConfig` içində `NewTopic` bean-ləri ilə əsas topic-lər əl ilə yaradılır
- Deklarativ non-blocking retry: consumer-lər üzərində `@RetryableTopic` ilə qurulur
- Programmatic non-blocking retry: `KafkaConfig` içində comment şəklində `RetryTopicConfigurationBuilder` nümunəsi saxlanılıb

Hazırkı aktiv işləyən retry axını annotation əsaslı `@RetryableTopic` mexanizmidir.

## Consumer axınları

### 1. UserCreatedConsumer

[src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java) aşağıdakı konfiqurasiyaya malikdir:

- topic: `user-created-event-topic`
- group id: `user-created-group`
- retry edilən exception: `RetryableException`
- attempts: `4`
- delay: `200`
- multiplier: `3`
- num partitions: `2`
- replication factor: `3`

Davranış:

- Mesaj əsas topic-də qəbul olunur
- Listener `RetryableException` atır
- Mesaj retry topic-lərinə ötürülür
- Bütün cəhdlər uğursuz olarsa mesaj DLT-yə düşür

### 2. UsernameChangedConsumer

[src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java) aşağıdakı konfiqurasiyaya malikdir:

- topic: `username-changed-event-topic`
- group id: `username-changed-group`
- listener daxilində atılan exception: `NotRetryableException`
- `@RetryableTopic` daxilində retry yalnız `RetryableException` üçün aktivdir
- num partitions: `2`
- replication factor: `3`

Davranış:

- Mesaj əsas topic-də qəbul olunur
- Listener `NotRetryableException` atır
- Bu exception `include = {RetryableException.class}` daxilində olmadığı üçün retry olunmur
- Mesaj birbaşa DLT-yə gedir

## Texniki axın

### 1. Producer

`UserController` daxil olan HTTP request-i `UserProducer`-ə ötürür.

Request:

```http
POST /api/v1/users
Content-Type: application/json
```

Body nümunəsi:

```json
{
  "name": "Hilal"
}
```

`UserProducer` aşağıdakı addımları edir:

- `UserCreatedRequest` obyektindən `UserCreatedEvent` yaradır
- random UUID ilə message key təyin edir
- `X-USER-ID` header əlavə edir
- mesajı `user-created-event-topic` topic-inə göndərir

Username change üçün ayrıca HTTP axını da var:

```http
POST /api/v1/users/username-changes
Content-Type: application/json
```

Body nümunəsi:

```json
{
  "oldUsername": "hilal",
  "newUsername": "hilal-dev"
}
```

Bu endpoint `UsernameChangedRequest` qəbul edir və `username-changed-event-topic` topic-inə `UsernameChangedEvent` göndərir.

`application.yml` daxilində producer üçün:

- `JsonSerializer`
- `spring.json.add.type.headers=true`

istifadə olunur.

Bu header consumer tərəfdə payload-un hansı Java class-a deserialize olunacağını müəyyən edir.

### 2. Non-blocking retry mexanizmi

`@RetryableTopic` blocking retry etmir. Yəni eyni record consumer thread-də uzun müddət saxlanmır.

Bunun əvəzinə Spring Kafka mesajı retry topic-lərinə publish edir. `UserCreatedConsumer` üçün axın belə görünür:

- əsas topic: `user-created-event-topic`
- 1-ci retry topic: `user-created-event-topic-retry-200`
- 2-ci retry topic: `user-created-event-topic-retry-600`
- 3-cü retry topic: `user-created-event-topic-retry-1800`
- son dayanacaq: `user-created-event-topic-dlt`

Buradakı delay-lər `delay=200` və `multiplier=3` əsasında yaranır.

`UsernameChangedConsumer` üçün isə listener `NotRetryableException` atdığına görə retry topic-lərinə keçid olmur və mesaj birbaşa DLT-yə yönlənir.

### 3. DLT

Hər iki consumer-də `@DltHandler` mövcuddur:

- `UserCreatedConsumer` DLT-də `UserCreatedEvent` qəbul edir
- `UsernameChangedConsumer` DLT-də `UsernameChangedEvent` qəbul edir

Hər iki halda original offset log olunur.

## Topic-lər

Layihədə aşağıdakı topic-ləri görəcəksiniz:

- `user-created-event-topic`
- `user-created-event-topic-retry-200`
- `user-created-event-topic-retry-600`
- `user-created-event-topic-retry-1800`
- `user-created-event-topic-dlt`
- `username-changed-event-topic`
- `username-changed-event-topic-dlt`

Qeyd:

- Əsas topic-lər `KafkaConfig` içində manual yaradılır
- Retry topic-ləri və DLT-lər `@RetryableTopic` tərəfindən avtomatik yaradıla bilər
- `@RetryableTopic` üzərində `numPartitions = "2"` və `replicationFactor = "3"` verilib
- `KafkaConfig` içində programmatic retry konfiqurasiyası üçün ayrıca nümunə də saxlanılıb

## Deserialize davranışı

Consumer konfiqurasiyasında aşağıdakılar var:

- `ErrorHandlingDeserializer`
- delegate olaraq `JsonDeserializer`
- `spring.json.trusted.packages: "*"`

Bu o deməkdir ki, deserialize xətası listener thread-i birbaşa qırmır və Spring Kafka xəta axınına ötürülür.

Bu repo-nun əsas fokusu business exception retry axını olsa da, JSON parse və type header problemlərini test etmək üçün də baz konfiqurasiya mövcuddur.

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

### 3. Test request göndərin

```bash
curl -X POST http://localhost:8085/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"name":"Hilal"}'
```

Bu request `UserCreatedEvent` flow-unu işə salır.

`UsernameChangedEvent` flow üçün:

```bash
curl -X POST http://localhost:8085/api/v1/users/username-changes \
  -H "Content-Type: application/json" \
  -d '{"oldUsername":"hilal","newUsername":"hilal-dev"}'
```

## Nəyə baxmaq lazımdır

`UserCreatedConsumer` üçün:

- Producer mesajı əsas topic-ə yazır
- Consumer mesajı qəbul edir və `RetryableException` atır
- Mesaj retry topic-lərinə ötürülür
- Son retry-dan sonra mesaj DLT-yə düşür
- `@DltHandler` həmin mesajı log edir

`UsernameChangedConsumer` üçün:

- Producer mesajı `username-changed-event-topic` topic-inə yazır
- Consumer mesajı qəbul edir və `NotRetryableException` atır
- Mesaj retry olmadan birbaşa DLT-yə gedir
- `@DltHandler` həmin mesajı log edir

Redpanda Console üzərindən topic və mesajları izləyə bilərsiniz:

- `http://localhost:8081`

## Test ssenariləri

### 1. Retryable axın

Heç bir kod dəyişmədən:

- `POST /api/v1/users` request göndərin
- retry topic-lərinin yarandığını görün
- mesajın sonda `user-created-event-topic-dlt` topic-inə getdiyini izləyin

### 2. Uğurlu emal ssenarisi

Əgər normal uğurlu emalı görmək istəsəniz, [src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java) daxilində `throw new RetryableException();` sətrini müvəqqəti silə bilərsiniz.

### 3. Non-retryable axın

Heç bir kod dəyişmədən:

- `POST /api/v1/users/username-changes` request göndərin
- mesaj `UsernameChangedConsumer` tərəfindən qəbul olunacaq
- `NotRetryableException` atılacaq
- mesaj retry edilmədən birbaşa `username-changed-event-topic-dlt` topic-inə düşəcək

## Faydalı ünvanlar

- App: `http://localhost:8085`
- Redpanda Console: `http://localhost:8081`
- Endpoints: `POST /api/v1/users`, `POST /api/v1/users/username-changes`
- Main topic: `user-created-event-topic`
- Secondary topic: `username-changed-event-topic`
- DLT topics: `user-created-event-topic-dlt`, `username-changed-event-topic-dlt`

---

## EN

# Kafka Non-Blocking Retry and DLT Demo

This project is a simple Spring Kafka example that demonstrates non-blocking retry and Dead Letter Topic (DLT) handling with `@RetryableTopic`.

The repository now contains two different consumer flows:

- `UserCreatedConsumer` demonstrates the retryable scenario
- `UsernameChangedConsumer` demonstrates the non-retryable scenario

So the same project shows both a retried failure and a direct-to-DLT failure.

## What this project demonstrates

- Manual topic configuration (`NewTopic`, `TopicBuilder`)
- Declarative retry configuration with `@RetryableTopic`
- A programmatic / imperative retry sample with `RetryTopicConfigurationBuilder`
- Non-blocking retry with `@RetryableTopic`
- Retry only for selected exceptions (`RetryableException`)
- A non-retryable exception example (`NotRetryableException`)
- Exponential backoff behavior: `200ms -> 600ms -> 1800ms`
- `numPartitions = 2` and `replicationFactor = 3` for retry and DLT topics
- DLT handling with `@DltHandler`
- Producer-side JSON type headers
- Consumer-side `ErrorHandlingDeserializer` + `JsonDeserializer`

## Main files

- [README.md](/Users/hilalhilalli/Desktop/kafka-example/README.md)
- [src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java)
- [src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java)
- [src/main/java/ourcorp/kafka/example/producer/UserProducer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/producer/UserProducer.java)
- [src/main/java/ourcorp/kafka/example/controller/UserController.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/controller/UserController.java)
- [src/main/java/ourcorp/kafka/example/config/KafkaConfig.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/config/KafkaConfig.java)
- [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml)

## Approaches used in this project

This repository is intentionally structured to show multiple configuration styles in one place:

- Manual topic configuration: the main topics are created explicitly in `KafkaConfig` with `NewTopic` beans
- Declarative non-blocking retry: configured on consumers with `@RetryableTopic`
- Programmatic non-blocking retry: an example based on `RetryTopicConfigurationBuilder` is kept in `KafkaConfig` as reference

The active retry flow in the current implementation is the annotation-based `@RetryableTopic` mechanism.

## Consumer flows

### 1. UserCreatedConsumer

[src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java) is configured with:

- topic: `user-created-event-topic`
- group id: `user-created-group`
- retryable exception: `RetryableException`
- attempts: `4`
- delay: `200`
- multiplier: `3`
- num partitions: `2`
- replication factor: `3`

Behavior:

- The message is consumed from the main topic
- The listener throws `RetryableException`
- The record is forwarded to retry topics
- If all attempts fail, the record is sent to the DLT

### 2. UsernameChangedConsumer

[src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UsernameChangedConsumer.java) is configured with:

- topic: `username-changed-event-topic`
- group id: `username-changed-group`
- exception thrown by the listener: `NotRetryableException`
- retry is enabled only for `RetryableException` in `@RetryableTopic`
- num partitions: `2`
- replication factor: `3`

Behavior:

- The message is consumed from the main topic
- The listener throws `NotRetryableException`
- Because that exception is not included in `include = {RetryableException.class}`, no retry happens
- The record goes directly to the DLT

## Technical flow

### 1. Producer

`UserController` accepts the HTTP request and forwards it to `UserProducer`.

Request:

```http
POST /api/v1/users
Content-Type: application/json
```

Example body:

```json
{
  "name": "Hilal"
}
```

`UserProducer` then:

- builds a `UserCreatedEvent` from `UserCreatedRequest`
- generates a random UUID as the message key
- adds the `X-USER-ID` header
- sends the message to `user-created-event-topic`

There is also a separate HTTP flow for username changes:

```http
POST /api/v1/users/username-changes
Content-Type: application/json
```

Example body:

```json
{
  "oldUsername": "hilal",
  "newUsername": "hilal-dev"
}
```

That endpoint accepts `UsernameChangedRequest` and publishes `UsernameChangedEvent` to `username-changed-event-topic`.

In [src/main/resources/application.yml](/Users/hilalhilalli/Desktop/kafka-example/src/main/resources/application.yml), the producer uses:

- `JsonSerializer`
- `spring.json.add.type.headers=true`

Those headers help the consumer deserialize the payload into the expected Java class.

### 2. Non-blocking retry mechanism

`@RetryableTopic` does not perform blocking retry in the same consumer thread. Instead, Spring Kafka republishes the failed record to retry topics.

For `UserCreatedConsumer`, the practical flow looks like this:

- main topic: `user-created-event-topic`
- first retry topic: `user-created-event-topic-retry-200`
- second retry topic: `user-created-event-topic-retry-600`
- third retry topic: `user-created-event-topic-retry-1800`
- final destination: `user-created-event-topic-dlt`

Those delay values are derived from `delay=200` and `multiplier=3`.

For `UsernameChangedConsumer`, the listener throws `NotRetryableException`, so the record does not move through retry topics and is sent directly to the DLT.

### 3. DLT

Both consumers define a `@DltHandler`:

- `UserCreatedConsumer` handles `UserCreatedEvent` from its DLT
- `UsernameChangedConsumer` handles `UsernameChangedEvent` from its DLT

In both cases, the original offset is logged.

## Topics

You will see the following topics in this project:

- `user-created-event-topic`
- `user-created-event-topic-retry-200`
- `user-created-event-topic-retry-600`
- `user-created-event-topic-retry-1800`
- `user-created-event-topic-dlt`
- `username-changed-event-topic`
- `username-changed-event-topic-dlt`

Notes:

- The main topics are created manually in `KafkaConfig`
- Retry topics and DLTs can be auto-created by `@RetryableTopic`
- `@RetryableTopic` is configured with `numPartitions = "2"` and `replicationFactor = "3"`
- The same config class also contains a programmatic retry example for reference

## Deserialization behavior

The consumer configuration includes:

- `ErrorHandlingDeserializer`
- delegated `JsonDeserializer`
- `spring.json.trusted.packages: "*"`

This means deserialization failures are pushed into Spring Kafka's error-handling path instead of crashing the listener directly.

Even though the main focus of this repository is retry behavior for business exceptions, the base deserialization setup is also in place for malformed JSON or wrong type-header scenarios.

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

This request triggers the `UserCreatedEvent` flow.

For the `UsernameChangedEvent` flow:

```bash
curl -X POST http://localhost:8085/api/v1/users/username-changes \
  -H "Content-Type: application/json" \
  -d '{"oldUsername":"hilal","newUsername":"hilal-dev"}'
```

## What to observe

For `UserCreatedConsumer`:

- The producer writes to the main topic
- The consumer reads the record and throws `RetryableException`
- The record is forwarded through retry topics
- After the final retry, the record is published to the DLT
- `@DltHandler` logs the DLT message

For `UsernameChangedConsumer`:

- The producer writes to `username-changed-event-topic`
- The consumer reads the record and throws `NotRetryableException`
- The record goes directly to the DLT without retry
- `@DltHandler` logs the DLT message

You can inspect topics and messages in Redpanda Console:

- `http://localhost:8081`

## Test scenarios

### 1. Retryable flow

Without changing any code:

- send `POST /api/v1/users`
- observe retry topic creation
- observe the record eventually reaching `user-created-event-topic-dlt`

### 2. Successful processing scenario

If you want to see the successful path, temporarily remove `throw new RetryableException();` from [src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java](/Users/hilalhilalli/Desktop/kafka-example/src/main/java/ourcorp/kafka/example/consumer/UserCreatedConsumer.java).

### 3. Non-retryable flow

Without changing any code:

- send `POST /api/v1/users/username-changes`
- the message is consumed by `UsernameChangedConsumer`
- `NotRetryableException` is thrown
- the record is sent directly to `username-changed-event-topic-dlt` without retry

## Useful endpoints

- App: `http://localhost:8085`
- Redpanda Console: `http://localhost:8081`
- Endpoints: `POST /api/v1/users`, `POST /api/v1/users/username-changes`
- Main topic: `user-created-event-topic`
- Secondary topic: `username-changed-event-topic`
- DLT topics: `user-created-event-topic-dlt`, `username-changed-event-topic-dlt`

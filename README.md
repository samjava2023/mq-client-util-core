# mq-client-util-core

Standalone IBM MQ client library for Java 8+. Not part of the `mq-client-util` multi-module parent.

## Build

```cmd
cd d:\project\AI\mq-client-util-core
mvn clean install
```

Installs `com.yourorg.mq:mq-client-util-core:1.0.1-SNAPSHOT` to your local Maven repository (`~/.m2`).

## Use in Spring Boot (Boot 1.4 / 2.6)

```xml
<dependency>
  <groupId>com.yourorg.mq</groupId>
  <artifactId>mq-client-util-core</artifactId>
  <version>1.0.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
</dependency>
```

See `..\mq-client-util\SPRING-BOOT-26-INTEGRATION.md` for full integration steps.

## Project layout

| Package | Purpose |
|---------|---------|
| `com.db.pwmus.mqclient.api` | `MqSender`, `MqListener`, `MqMessage` |
| `com.db.pwmus.mqclient.core` | `MqClientFactory`, `MqClient` facade |
| `com.db.pwmus.mqclient.spi` | `MqProvider` plug-in interface |
| `com.db.pwmus.mqclient.ibmmq` | IBM MQ implementation |
| `com.db.pwmus.mqclient.spring` | `@EnableMqClient` (optional Spring) |

<img src="https://www.conduktor.io/svgs/logo/black.svg" width="256">

This internal technical POC demonstrates how to work with the Kafka protocol to alter incoming/outgoing records. Our original aim was to understand how to encrypt or validate data. This POC only contain the capability to log what is happening at the protocol level, but it's extensible. We've decided to open-source it for educational purposes only; it is not meant to be used, especially not in production.

# Why?

This POC demonstrates how to transform Kafka requests and responses between clients and Apache Kafka.
In particular, we wanted to demonstrate how to encrypt/decrypt data on the fly and add any kind of transformations to the records.

> This POC was the premise of our product called Conduktor Gateway. You can find its [documentation here](https://docs.conduktor.io/gateway/). As this is quite a technical product, we recommend you book a demo with us. We'll be happy to help you and your team get started with the best practices and the best setup for your use case.

We do not really accept contributions as it was just a POC. There is no effort on our side to maintain it.

# How to start?

By default, this POC only logs lines and information about the Kafka traffic to stdout.

- Build
```java
mvn clean package # requires Java 17+
```

- Update `gateway-core/config/kafka.config` and update `bootstrap.servers` to point to your Kafka cluster (e.g., `localhost:9092`). (examples in `/gateway-core/config`)

- Update `application.yaml` to setup the application:

```yaml
kafkaSelector:
  type: file
  path: config/kafka.config 

interceptors:
- name: loggingInterceptor
  pluginClass: io.conduktor.example.loggerinterceptor.LoggerInterceptorPlugin
  timeoutMs: 30000
  priority: 100
  config:
    - key: "loggingStyle"
      value: "obiWan"

#hostPortConfiguration:
#  gatewayBindHost: 0.0.0.0
#  gatewayHost: localhost
#  portRange: 6969:6975

#authenticationConfig:
#  authenticatorType: NONE
#  exponentialBackoff:
#    multiplier: 2
#    backoffMaxMs: 5000
#  sslConfig:
#    updateContextIntervalMinutes: 5
#    keyStore:
#      keyStorePath: config/kafka-gateway.keystore.jks
#      keyStorePassword: pwd
#      keyPassword: pwd
#      keyStoreType: jks
#      updateIntervalMsecs: 600000

#threadConfig:
#  downStreamThread: 2
#  upstream:
#    numberOfThread: 2
#    maxPendingTask: 2048

#maxResponseLatency: 3000
#inFlightRequestExpiryMs: 30000

#upstreamConnectionConfig:
#  numOfConnection: 10
#  maxIdleTimeMs: 200000
```

- Change your applications to point to `localhost:6969` and see the traffic flowing!
- Add capabilities like encryption and reference it in the `interceptors` list.

# It's a POC

This POC was the premise of our product called Conduktor Gateway. You can find its [documentation here](https://docs.conduktor.io/gateway/). As this is quite a technical product, we recommend you book a demo with us. We'll be happy to help you and your team get started with the best practices and the best setup for your use case.

# License

This project is licensed under the [Conduktor Community License](https://www.conduktor.io/conduktor-community-license-agreement-v1.0).



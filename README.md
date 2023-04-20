# Conduktor Gateway — a Kafka transformer platform

Conduktor Gateway is a tool to intercept and then filter and transform requests and responses between clients and Apache
Kafka. For example, an interceptor may add headers to messages as they are produced to Kafka or may filter out one particular
type of message on consume.

Conduktor Gateway makes it simple to quickly define interceptors that can be combined into pipelines that will be applied to
client interactions.

# How to start?

Conduktor Gateway comes with an included interceptor which writes log lines and information about the Kakfka traffic to stdout.

The following Quick Start steps guide you through setting up the Gateway, the interceptor, and validating the configuration.

### Get a Kafka instance:

Follow step 1 and step 2 (Zookeeper or KRaft) from the  [Apache Kafka Quick Start](https://kafka.apache.org/quickstart) to start a single broker cluster running on your local system.

OR

Get the connection information for your existing Kafka cluster

### Download and build

```bash
$ git clone https://github.com/conduktor/conduktor-gateway && cd conduktor-gateway
$ code gateway-core/config/kafka.config   # Use the defaults, or update to include your existing Kafka cluster configuration.
$ mvn clean package                # build the .jar, requires Java 17+
```

### Start Conduktor Gateway:

```bash
$ export CLASSPATH=logger-interceptor/target/logger-interceptor-0.1.0-SNAPSHOT.jar # Add the logger interceptor to the classpath, or add your own interceptor jar files here
$ bin/run-gateway.sh
```

### Validate

Test the APIs using the command line tools
```bash
cd <kafka_install_directory>
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:6969
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:6969
bin/kafka-console-consumer.sh --topic quickstart-events --bootstrap-server localhost:6969 --from-beginning --property print.headers=true
```

Then look in the terminal where the gateway is running, you should find log lines, written by the loggerInterceptor, similar to:

```
Hello there, a class org.apache.kafka.common.requests.FetchResponse was sent/received 
Produce was called with version: 9
Fetch was requested from localhost
```

OR

Update your Kafka Clients' `bootstrap.servers` to `localhost:6969` (the Gateway) and remove any authentication configuration. Note that Conduktor Gateway does not use the same authentication scheme as the backing Kafka cluster and so clients should be configured according to the authenticatorType configuration rather than what is configured in the Kafka cluster.

Run your Kafka Client workload, and look for the log lines written to the terminal where Gateway is running.  These will include information about the flows passing through the Gateway from your application.

# How to contribute to this project?

This project contains everything you need to get started with Conduktor Gateway including:

* The core gateway module
* The interceptor framework - a set of interfaces to be implemented in your own interceptor project
* An example interceptor implementation
* Integration tests - these verify the core module and provide a platform for testing custom interceptors.

# Project layout:

```
conduktor-gateway    
│
└── interceptor-framework - a minimal template for creating interceptors
│
└── logger-interceptor - an example interceptor project 
│
└── gateway-core - the main gateway project 
│   │
│   └── config - sample configuration files
│   │
│   └── src/main/java - the code
│   │
│   └── src/test/java - unit tests
│   
└── gateway-test - integration testing
│   │
│   └── config - configuration files for test cases
│   │
│   └── src/test/java - integration tests
│   │
│   └── src/test/resources - docker setup for integration tests 
```

# Building

Conduktor Gateway is a simple maven project and can be built with:

```
mvn clean package
```

# Running

## Preparation

To run conduktor-gateway you will first require:

* Java 17+
* a running kafka cluster
* an updated **gateway-core/config/kafka.config** to point to your kafka cluster.

## Configuration

Conduktor Gateway requires 2 configuration files, examples found in `/gateway-core/config`

`kafka.config` holds the configuration the gateway should use to connect to the backing Kafka cluster. This should 
contain similar information and formatting to any Java client connecting to the backing Kafka. e.g.:

```
bootstrap.servers=localhost:9092
```

`application.yaml` contains gateway specific configuration options, descriptions of these can be found below:

### Kafka Connection Configurations
```yaml
kafkaSelector:
  file:
    path:  - the path to the file containing Kafka connection properties e.g. config/kafka.config
```

### Host/Port Configurations
```yaml
hostPortConfiguration:
  gatewayBindHost: - the host to bind on e.g. 0.0.0.0
  gatewayHost: - the host name of the gateway to be returned to clients e.g. localhost
  portRange: - a port range that gateway can assign to brokers e.g. 6969:6975
```

### Authentication Configurations
```yaml
authenticationConfig:
  authenticatorType: - the authentication type for client <--> gateway e.g. NONE
  exponentialBackoff:
    multiplier: - backoff multiplier for failed authentication attempts e.g. 2
    backoffMaxMs: - maximum backoff time e.g. 5000
  sslConfig:
    updateContextIntervalMinutes: - the interval to check for for SSL context changes e.g. 5
    keyStore:
      keyStorePath: - path to a SSL keystore  e.g. config/kafka-gateway.keystore.jks
      keyStorePassword: - the keystore password 
      keyPassword: - the key password
      keyStoreType: - the keystore type e.g. jks
      updateIntervalMsecs: - the interval to check for keystore changes e.g. 600000
```

### Thread Configurations
```yaml
threadConfig:
  downStreamThread: - the thread pool size for handling downstream connections e.g. 2
  upstream:
    numberOfThread: - the thread pool size for handling upstream connections e.g. 2
    maxPendingTask: - the maximum pending upstream takss before new tasks will be rejected e.g. 2048
```

### Request Handling Configurations
```yaml
maxResponseLatency: - the maximum time gateway will wait for a response from Kafka e.g. 3000
inFlightRequestExpiryMs: - the maximum total time to process a request in gateway before it will be rejected e.g. 30000
```

### Upstream Connection Configurations

```yaml
upstreamConnectionConfig:
  numOfConnection: - the number of connections made to each upstream Kafka broker e.g. 10
  maxIdleTimeMs: - the maximum time a connection can remain idle before it will be reaped e.g. 200000
```

Alternatively, these configs can be provided using environment variables as follows:

### Host/Port Configurations

| Environment Variable | Default Value                                              | Description                                                                                                                                                                                                                                                                                                     |
|----------------------|------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `GATEWAY_BIND_HOST`  | `0.0.0.0`                                                  | The host on which to bind the gateway                                                                                                                                                                                                                                                                           |
| `GATEWAY_HOST`       | `localhost`                                                | The gateway hostname that should be presented to clients in the Apache Kafka Response flows (similar to advertised listener)                                                                                                                                                                                    |
| `GATEWAY_PORT_RANGE` | `6969:6975`                                                | A range of ports to be opened on the Conduktor Gateway host, each port in this range will correspond to a broker in the Kafka cluster so it must be at least as large as the broker count of the Kafka cluster. We recommend it is double the size of the Kafka cluster to allow for expansion and reassignment |

### Authentication Configurations

Note: These configurations apply to authentication between clients and Conduktor Gateway. For authentication between Conduktor Gateway and Kafka see `kafka.config`

| Environment Variable | Default Value | Description                                                                                                                                                                                                                                                    |
|----|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AUTHENTICATION_AUTHENTICATOR_TYPE` | `NONE`        | The type of authentication clients should use to connect to the Gateway, valid values are NONE (no authentication, no encryption), SSL (tls encryption), SASL_PLAIN (sasl authentication, no encryption) and SASL_SSL (sasl authentication and tls encryption) |
| `SSL_KEY_STORE_PATH` | None          | Path to a keystore for SSL connections                                                                                                                                                                                                                         |
| `SSL_KEY_STORE_PASSWORD` | None          | Password for the keystore defined above                                                                                                                                                                                                                        |
| `SSL_KEY_PASSWORD` | None          | Password for the key contained in the store above                                                                                                                                                                                                              |
| `SSL_KEY_TYPE` | `jks`          | The type of keystore used for SSL connections, valid values are jks or pkcs12                                                                                                                                                                                  |

### Thread Configurations

| Environment Variable | Default Value | Description                                                                          |
|----|---------------|--------------------------------------------------------------------------------------|
| `DOWNSTREAM_THREAD` | `2`           | The number of threads dedicated to handling IO between clients and Conduktor Gateway |
| `UPSTREAM_THREAD` | `4`           | The number of threads dedicated to handling IO between Kafka and Conduktor Gateway   |

### Upstream Connection Configurations

| Environment Variable | Default Value | Description                                                               |
|----|---------------|---------------------------------------------------------------------------|
| `UPSTREAM_NUM_CONNECTION` | `10`           | The number of connections between Conduktor Gateway and each Kafka broker |

## Configuring Interceptors

Interceptors are configured in `application.yaml` under the `interceptors` field. Each interceptor definition must have the following properties:

```yaml
name: - A unique name used to describe this interceptor
pluginClass: - The fully qualified Java class name of the Plugin implementation that defines this interceptor
priority: - an integer value signifying the ordering of this interceptor compared with others (0 is highest priority)
timeoutMs: - interceptor timeout (millisecond), this value is optional and defaults to 30s
config: - a set of key value pairs that will be passed to the interceptor when created.
```

For example:

```yaml
interceptors:
- name: loggingInterceptor
  pluginClass: io.conduktor.example.loggerinterceptor.LoggerInterceptorPlugin
  timeoutMs: 30000
  priority: 100
  config:
    - key: "loggingStyle"
      value: "obiWan"
```
# License

This project is licensed under the [Conduktor Community License](https://www.conduktor.io/conduktor-community-license-agreement-v1.0).



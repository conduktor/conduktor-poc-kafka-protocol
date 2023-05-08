
# Interceptors

There are a number of interceptors already available as part of the Conduktor Marketplace.  If one of these does not fulfil your requirements, you can create your own interceptor using the Interceptor API.

This page describes the steps required to create your own interceptor, ready to deploy to your environment, or release to the Conduktor Marketplace for the wider community to enjoy.

## What is the Conduktor Gateway

The Conduktor Gateway is a powerful tool that sits between your Kafka clients and your Kafka clusters. This gateway hosts plugable, extensible interceptors, which can be configured to have access to all Kafka traffic passing through the Gateway.

Interceptors are configured to trigger actions based on the type and content of Kafka traffic. From manipulating the Kafka data itself, to interacting with an external service, what can be achieved by being able to act on any of your in-flight Kafka data is limited only by your imagination.

Interceptors are configured in the Conduktor Gateway's [application.yaml](../gateway-core/config/application.yaml) configuration file by specifying a fully qualified class name, configuration details and a priority.  Multiple interceptors can be configured to run sequentially, in the priority specified order.  The project [README.md](../README.md) documents configuring and starting the Gateway.

If there is no interceptor available that fulfils your requirements, you can easily add your own.

## Fundamentals of Interceptor Development

The Interceptor Framework defines the two interfaces required to define your own Interceptors: [Interceptor.java](/src/main/java/io/conduktor/gateway/interceptor/Interceptor.java) and [Plugin.java](/src/main/java/io/conduktor/gateway/interceptor/Plugin.java).

### `Interceptor.java`
The `Interceptor.java` interface defines a method that takes a Kafka request or response object, which can then be used to drive that interceptor function.

The Kafka requests or responses are objects that inherit from Kafka’s [AbstractRequestResponse](https://github.com/a0x8o/kafka/blob/master/clients/src/main/java/org/apache/kafka/common/requests/AbstractRequestResponse.java) type. For example a [ProduceRequest](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java) or a [FetchResponse](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/FetchResponse.java).

An interceptor is likely to contain multiple implementations of the `Interceptor.java` interface.  Each of these implementations work on different types of Kafka requests or responses.  The interface implementations within an Interceptor should all have the same general purpose, such as encryption, logging or adding a message header.

For example, a logging Interceptor may include two `Interceptor.java` implementations; one that takes a `ProduceRequest` Java object and logs information about any produced records, and a separate `Interceptor.java` implementation that takes a `FetchResponse` Java object and logs information about any consumed records.

These are separate implementation classes as the processing of a `ProduceRequest` object is quite different to the processing on a `ConsumeResponse` object

### `InterceptorProvider.java`
The `InterceptorProvider.java` is a record class that provide an `Interceptor` and the target Kafka type to intercept.

### `Plugin.java`
An `Plugin` returns a list of all the interceptors in the shape of `InterceptorProvider` that combine to make the function provided by the `Plugin`.

It also processes the interceptor configuration, which is specified in `application.yaml`.

### Interceptor framework jar

The Interceptor framework jar file contains the `Interceptor.java` implementations and the `Plugin.java` implementation.  The jar file should be added to the classpath of your gateway to provide access to the Interceptor functionality.

# Details

## `Interceptor.java`

The `Interceptor.java` interface defines the API that is used to intercept a Kafka Request or Response:

```java
CompletionStage<T> intercept(T input, InterceptorContext interceptorContext) {
   //Interceptor code here
}
```
where
```java
<T> extends AbstractRequestResponse
```

There are three conceptual levels for how specific the interceptor type `<T>` match is: 

 - Specific requests or responses, where `<T>` is any of the specific request or response classes (eg `FetchResponse`)
 - All requests or all responses, where `<T>` is `AbstractRequest` or `AbstractResponse`
 - All flows, where `<T>` is `AbstractRequestResponse`.

When writing a new interceptor, use the most specific type of `<T>` that is suitable for your needs.

The next section describes how to determine what to use for `<T>`:

### Intercept specific request or response types

To intercept a specific request or response, specify the exact type of the request or response in the method declaration. For example

```java
CompletionStage<FetchResponse> intercept(FetchResponse input, InterceptorContext interceptorContext)`
```

**Important:** Only specify one instance of the intercept method in each implementation class.

It is not valid to have more than one intercept method, this will result in only one of the interceptor methods being run when Kafka API flows are processed.  The following example is not valid:

```java
CompletionStage<FetchResponse> intercept(FetchResponse input, InterceptorContext interceptorContext) { }
CompletionStage<ProduceRequest> intercept(ProduceRequest input, InterceptorContext interceptorContext) { }
```

Instead, make two separate Java class files that each implement the `Interceptor.java` interface, specifying their own type. For the above example create both

```java
public class FetchLoggerInterceptor implements Interceptor<FetchResponse> {
   CompletionStage<FetchResponse> intercept(FetchResponse input, InterceptorContext interceptorContext) {}
}
```
and
```java
public class ProduceLoggerInterceptor implements Interceptor<ProduceRequest> {
   CompletionStage<ProduceRequest> intercept(ProduceRequest input, InterceptorContext interceptorContext) {}
}
```
Then use `Plugin` to provide both implementations to the Gateway to register it for use:

```java
public class LoggerInterceptorPlugin implements Plugin {
   public List<InterceptorProvider<?>> getInterceptors(Map<String, Object> config) throws InterceptorConfigurationException {
      return List.of(
         new InterceptorProvider<>(FetchResponse.class, new FetchLoggerInterceptor()),
         new InterceptorProvider<>(ProduceRequest.class, new ProduceLoggerInterceptor())
      );
   }
}
```

You might want to intercept specific requests or responses if you have a very targeted interceptor that is only applicable to a handful or Kafka API flows, or if each type of request or response being processed has a different type so needs to be handled differently.

### Intercept all requests or all responses

To intercept all requests or all responses, specify the generic `AbstractRequest` or `AbstractResponse` as the type in the method declaration.

```java
public class RequestsLoggerInterceptor implements Interceptor<AbstractRequest> {
  CompletionStage<AbstractRequest> intercept(AbstractRequest input) {}
}
```

Then use the `Plugin` to provide this implementation the Gateway to register it for use:

```java
public class LoggerInterceptorPlugin implements InterceptorPlugin {
   public List<InterceptorProvider<?>> getInterceptors(Map<String, Object> config) throws InterceptorConfigurationException {
      return List.of(
         new InterceptorProvider<>(AbstractRequest.class, new RequestsLoggerInterceptor())
      );
   }
}
```

You should intercept `AbstractRequest` or `AbstractResponse` if the interceptor needs to work in the same way on all requests or all responses.  For example, you might want to process all requests to count the number of request bytes passing through the Gateway to Kafka, this would use an `AbstractRequest`.

### Intercept all traffic (both requests and responses)

To intercept all requests and all responses, specify the generic `AbstractRequestResponse` as the type in the method declaration.

```java
public class AllLoggerInterceptor implements Interceptor<AbstractRequestResponse> {
   CompletionStage<AbstractRequest> intercept(AbstracAbstractRequestResponsetRequest input) {}
}
```

Then use the `Plugin` to provide this implementation to the Gateway:

```java
public class LoggerInterceptorPlugin implements Plugin {
   public List<InterceptorProvider<?>> getInterceptors(Map<String, Object> config) throws InterceptorConfigurationException {
      return List.of(
         new InterceptorProvider<>(AbstractRequestResponse.class, new AllLoggerInterceptor())
      );
   }
}
```

You should intercept `AbstractRequestResponse` if the interceptor needs to work on all requests and responses.  For example, you might want to write an audit record for all requests and responses passing through the gateway.

### Intercept but want to break the flow

For some reasons, we might want to fast return the response to client without send the request to Kafka server.
Interceptor can throw a **InterceptorIntentionException**. See [Error handling](###error-handling) for more detail.

### CompletionStage

The `intercept` method on the `Interceptor.java` interface returns a `CompletionStage` to the Gateway.  This holds the Kafka request or response that was passed in to the intercept method.  The request or response may have been updated if this interceptor is one that manipulates the Kafka data, or it may remain unchanged if the interceptor does not manipulate the data.

The interceptor’s `CompletionStage` may hold a synchronous or asynchronous computation result.

For example, an synchronous logging interceptor would return a completed `Future` as its `CompletionStage`:

```java
return CompletableFuture.completedFuture(input)
```

An interceptor that adds data to a Kafka request based on a query to an external database would return a `Future`, holding the yet to be completed computation on the input Kafka request or response. This example also updates the original request or response input:

```java
CompletableFuture<AbstractRequestResponse> completableFuture = new CompletableFuture<>();

//Run the task asynchronously
Executors.newCachedThreadPool().submit(() -> {
   //Call out to database
   //Update input based on the slow database call
   //Return the updated input by completing the future
   completableFuture.complete(input);
});

return completableFuture;
```

**Important:**  The `Future` held in the `CompletionStage` must be completed by the intercept method.  If the `Future` is not eventually completed, then processing of Kafka API requests will be blocked and they will not be sent to Kafka.

Remember to handle completion of the `Future` for error cases as well as success cases.

### InterceptorContext

`InterceptorContext.java` acts as a container for information about the requests and responses that are being intercepted. This information can be useful to `Interceptor` implementations. For instance, an auditing interceptor may wish to record the hostname of connected clients.

It holds the following info:

* direction - The direction of the request (REQUEST or RESPONSE)
* requestHeader - The Kafka [RequestHeader](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/RequestHeader.java) associated with the requests that triggered this request/response.
* clientAddress - The address of the client that created this request (host/port)
* inFlightInfo - A Map to store extra information your interceptor may wish to pass on to subsequent interceptors in the chain. Typically, this is used to pass information between REQUEST interceptors and their corresponding RESPONSE interceptors. For instance an audit interceptor for FindCoordinator may wish to store details of the group id requested in the REQUEST in inFlightInfo as the RESPONSE does not contain this information. The map is keyed with String to provide an easy identifier for retrieval but the values can be any Object.

### Interceptor order

Individual interceptors are triggered in the order specified by their priority, with 0 being the highest priority and executed first. Negative priorities are not allowed.

If two interceptors have the same priority then order of execution is indeterminate.

Only interceptors applicable to the type of the request or response are triggered for a particular Kafka flow.

For example, a Kafka `FetchRequest` API request arriving with the Gateway will trigger interceptors where the type of the intercept method parameter is `FetchRequest`, `AbstractRequest` or `AbstractRequestResponse`.

The next interceptor in the prioritised list of applicable interceptors will not run until the previous interceptor’s `CompletionStage` has run and the associated `Future` has completed.

### Error handling

Conduktor Gateway catch exception from interceptor by two ways:

* try catch as normal flow
* using exceptionally callback of CompletionStage

So, we can be sure that if an interceptor get exception while executing, only current request get affected.

Beside of unexpected exception, we have **InterceptorIntentionException**. This is a special exception, which has a response inside. 
When Conduktor Gateway encounters this type of exception, it will get the response inside and return that to client.


## `Plugin.java`

`Plugin` defines one method:

```java
List<InterceptorProvider<?>> getInterceptors(Map<String, Object> config) throws InterceptorConfigurationException;
```

The `getInterceptors` method is used to populate the list of interceptors that this Interceptor jar provides based on the interceptor configuration read from `application.yaml`.  

### `getInterceptors`

The `getInterceptors` method is used to provide a list of interceptors to the Gateway based on the interceptor configuration read from `application.yaml`.  

The interceptors could be provided conditionally based on configuration, for example a logging interceptor could have a parameter “incoming_only : boolean”.  If only incoming requests are to be logged, then `getInterceptors` only returns the list of interceptors that apply to incoming requests.

**Note:** Remember that `AbstractRequest` is applicable to both requests and responses.

## Register your plugins

All implementation `Plugin` should be registered as services following Java [ServiceLoader](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html).

For that you need to add a file into `META-INF/services/io.conduktor.gateway.interceptor.Plugin` with all your implementations.


# Next Steps
Once you have implemented the `Interceptor.java` and `Plugin.java` interfaces, the next step is to build your changes into a standalone jar file providing your plugins as Java services.

The pom.xml in the loggerInterceptor package demonstrates one way to do this.

Place the built interceptor jar file on the classpath of the gateway.

Restart the gateway with the new jar file on the classpath.  In this example, the new interceptor is in a jar file that can be found in the conduktor-gateway repository under `myNewInterceptor/target/proxy-1.0-SNAPSHOT.jar`.

```bash
$ cp myNewInterceptor/target/my-new-interceptor-1.0-SNAPSHOT.jar bin/my-new-interceptor-1.0-SNAPSHOT.jar # Best practice is to store your interceptors in a central location
$ export CLASSPATH=$CLASSPATH:bin/my-new-interceptor-1.0-SNAPSHOT.jar
$ bin/run-gateway.sh
```

# FAQS

## Failed to load interceptors

When attempting to start the Gateway, the following exception is seen:
```
 Failed to load interceptors
java.lang.ClassNotFoundException: io.conduktor.example.MyPlugin
	at jdk.internal.loader.BuiltinClassLoader.loadClass(BuiltinClassLoader.java:641) ~[?:?]
```

This means that the jar file for the interceptor is not available on the classpath of the running JVM.

Ensure you are not running using the java -jar command, as this overwrites any classpath settings you have included.
Check you have specified the fully qualified class name of the Plugin correctly.
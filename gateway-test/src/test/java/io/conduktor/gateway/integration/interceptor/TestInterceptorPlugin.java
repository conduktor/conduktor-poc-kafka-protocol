package io.conduktor.gateway.integration.interceptor;

import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.InterceptorContext;
import io.conduktor.gateway.interceptor.Plugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.FetchRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TestInterceptorPlugin implements Plugin {
    private String prefix;
    @Override
    public void configure(Map<String, Object> config) {
        var loggingStyle = config.get("loggingStyle");
        if (loggingStyle.equals("obiWan")) {
            this.prefix = "Hello there";
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<Interceptor> getInterceptors() {
        return List.of(new AllLoggerInterceptor(prefix),
                new FetchRequestLoggerInterceptor());
    }

    @Slf4j
    public static class AllLoggerInterceptor implements Interceptor<AbstractRequestResponse> {

        private final String prefix;

        public AllLoggerInterceptor(String prefix) {
            this.prefix = prefix;
        }
        @Override
        public CompletionStage<AbstractRequestResponse> intercept(AbstractRequestResponse input, InterceptorContext interceptorContext) {
            log.warn("{}, a {} was sent/received", prefix, input.getClass());
            return CompletableFuture.completedFuture(input);
        }
    }

    @Slf4j
    public static class FetchRequestLoggerInterceptor implements Interceptor<FetchRequest> {
        @Override
        public CompletionStage<FetchRequest> intercept(FetchRequest input, InterceptorContext interceptorContext) {
            var source = interceptorContext.clientAddress().getHostName();
            log.warn("Fetch was requested from {}", source);
            interceptorContext.inFlightInfo().put("source", source);
            return CompletableFuture.completedFuture(input);
        }
    }

}

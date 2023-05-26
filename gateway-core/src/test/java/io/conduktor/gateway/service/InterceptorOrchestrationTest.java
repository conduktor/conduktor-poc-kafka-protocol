package io.conduktor.gateway.service;

import io.conduktor.gateway.interceptor.DirectionType;
import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.InterceptorContext;
import io.conduktor.gateway.interceptor.InterceptorValue;
import io.conduktor.gateway.model.InterceptContext;
import io.conduktor.gateway.model.User;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class InterceptorOrchestrationTest {

    private static InterceptorPoolService interceptorPoolService;
    private static InterceptorOrchestration interceptorOrchestration;
    private static InterceptContext interceptContext;

    @BeforeAll
    public static void setup() {
        interceptorPoolService = Mockito.mock(InterceptorPoolService.class);
        interceptorOrchestration = new InterceptorOrchestration(interceptorPoolService);

        interceptContext = Mockito.mock(InterceptContext.class);
        when(interceptContext.getDirectionType())
                .thenReturn(DirectionType.RESPONSE);


        var clientRequest = Mockito.mock(ClientRequest.class);
        when(clientRequest.getUser())
                .thenReturn(User.ANONYMOUS);

        var socketChannel = Mockito.mock(NioSocketChannel.class);
        when(interceptContext.getClientRequest())
                .thenReturn(clientRequest);
        when(clientRequest.getClientChannel())
                .thenReturn(socketChannel);
    }

    @Test
    public void testInterceptor_shouldErrorTimeout() {
        when(interceptorPoolService.getAllInterceptors(any()))
                .thenReturn(List.of(new InterceptorValue((Interceptor) new InterceptorTest(), 1, 500L)));

        var produceRequest = new FetchResponse(new FetchResponseData());
        assertThatThrownBy(() -> interceptorOrchestration.intercept(interceptContext, produceRequest)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS))
                .cause()
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    public void testInterceptor_shouldSuccess() throws Exception {
        when(interceptorPoolService.getAllInterceptors(any()))
                .thenReturn(List.of(new InterceptorValue((Interceptor) new InterceptorTest(), 1, 2000L)));

        var produceRequest = new FetchResponse(new FetchResponseData());
        var result = interceptorOrchestration.intercept(interceptContext, produceRequest)
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        assertThat(result).isNotNull()
                .isEqualTo(produceRequest);
    }

    public static class InterceptorTest implements Interceptor<FetchResponse> {

        @Override
        public CompletionStage<FetchResponse> intercept(FetchResponse input, InterceptorContext interceptorContext) {
            var afterTenSecs = delayedExecutor(1000, TimeUnit.MILLISECONDS);
            return CompletableFuture.supplyAsync(() -> "someValue", afterTenSecs)
                    .thenApply(rs -> input);
        }
    }
}

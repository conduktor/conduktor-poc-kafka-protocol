/*
 * Copyright 2023 Conduktor, Inc
 *
 * Licensed under the Conduktor Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * https://www.conduktor.io/conduktor-community-license-agreement-v1.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.conduktor.gateway.integration.interceptor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.InterceptorContext;
import io.conduktor.gateway.interceptor.InterceptorValue;
import io.conduktor.gateway.service.InterceptorPoolService;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class LoggerInterceptorPluginIntegrationTest extends BaseGatewayIntegrationTest {

    public static final String SOME_KEY = "someKey";
    public static final String SOME_VALUE = "someValue";
    public static final String LOGGERINTERCEPTOR_PACKAGE = "io.conduktor.gateway.integration.interceptor";

    private InterceptorPoolService mockInterceptorPoolService;

    @BeforeEach()
    public void setup() {
        super.reconfigureClientFactory(clientFactory);
        //reset mockInterceptorPoolService
        Mockito.doAnswer(InvocationOnMock::callRealMethod)
                .when(mockInterceptorPoolService).getAllInterceptors(any());
    }
    @Test
    public void testLoadsInterceptors() {
        assertThat(getInterceptorPoolService().getAllInterceptors(FetchRequest.class).size())
                .isEqualTo(2);
        assertThat(getInterceptorPoolService().getAllInterceptors(AbstractRequestResponse.class).size())
                .isEqualTo(1);
        assertThat(getInterceptorPoolService().getAllInterceptors(AbstractResponse.class).size())
                .isEqualTo(1);
    }

    @Test
    public void testInterceptsProduceMessages() throws ExecutionException, InterruptedException {

        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);

        var logStream = new ByteArrayOutputStream();
        var logWriter = new PrintWriter(logStream);
        addAppender(logWriter, "testInterceptsProduceMessages");
        Configurator.setLevel(LOGGERINTERCEPTOR_PACKAGE, Level.DEBUG);

        try (var gatewayProducer = clientFactory.gatewayProducer()) {
            ProducerRecord<String, String> record1 = new ProducerRecord<>(clientTopic, SOME_KEY, SOME_VALUE);
            gatewayProducer.send(record1).get();
        }

        logWriter.flush();
        // global interceptor
        assertThat(logStream.toString()
                .contains("Hello there, a class org.apache.kafka.common.requests.ProduceResponse was sent/received"))
                .isEqualTo(true);
    }

    @Test
    public void testInterceptsFetchMessages() throws ExecutionException, InterruptedException {

        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);

        var logStream = new ByteArrayOutputStream();
        var logWriter = new PrintWriter(logStream);
        addAppender(logWriter, "testInterceptsFetchMessages");
        Configurator.setLevel(LOGGERINTERCEPTOR_PACKAGE, Level.DEBUG);

        try (var gatewayConsumer = clientFactory.gatewayConsumer(getGroup())) {
            gatewayConsumer.subscribe(Collections.singletonList(clientTopic));
            IntStream.range(0,100).forEach(index ->
            gatewayConsumer.poll(Duration.ofMillis(100)));
        }

        logWriter.flush();
        // global interceptor
        assertThat(logStream.toString()
                .contains("Hello there, a class org.apache.kafka.common.requests.LeaveGroupResponse was sent/received"))
                .isEqualTo(true);
        // fetch interceptor
        assertThat(logStream.toString()
                .contains("Fetch was requested from localhost"))
                .isEqualTo(true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInterceptsFetchMessagesHanging() throws Exception {
        //Mock interceptor to delay = 1 DAY, and set timeoutMs of interceptor = 3s
        Mockito.doAnswer(invocationOnMockInterceptorValue -> {
                    var interceptorValues = (List<InterceptorValue>) invocationOnMockInterceptorValue.callRealMethod();
                    var mockInterceptorValues = new ArrayList<InterceptorValue>();
                    for (var interceptorValue : interceptorValues) {
                        var mockInterceptorValue = Mockito.spy(interceptorValue);
                        //mock timeout ms
                        Mockito.doReturn(3_000L)
                                .when(mockInterceptorValue)
                                .timeoutMs();

                        //mock delay to fetch request and response
                        Mockito.doAnswer(invocationOnMockInterceptor -> {
                                    var interceptor = (Interceptor) invocationOnMockInterceptor.callRealMethod();
                                    var mockInterceptor = Mockito.spy(interceptor);
                                    Mockito.doAnswer(invocationOnMock -> {
                                                var input = (AbstractRequestResponse) invocationOnMock.getArgument(0);
                                                //only delay FetchRequest and FetchResponse
                                                if (input instanceof FetchRequest || input instanceof FetchResponse) {
                                                    var afterTenSecs = delayedExecutor(1, TimeUnit.DAYS);
                                                    return CompletableFuture.supplyAsync(() -> "someValue", afterTenSecs)
                                                            .thenApply(rs -> input);
                                                } else {
                                                    return invocationOnMock.callRealMethod();
                                                }
                                            }).when(mockInterceptor)
                                            .intercept(any(AbstractRequestResponse.class), any(InterceptorContext.class));
                                    return mockInterceptor;
                                }).when(mockInterceptorValue)
                                .interceptor();

                        //replace interceptorValues with mockInterceptorValues
                        mockInterceptorValues.add(mockInterceptorValue);
                    }
                    return mockInterceptorValues;
                })
                .when(mockInterceptorPoolService).getAllInterceptors(any());

        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);

        var logStream = new ByteArrayOutputStream();
        var logWriter = new PrintWriter(logStream);
        addAppender(logWriter, "testInterceptsFetchMessagesHanging");
        Configurator.setLevel(LOGGERINTERCEPTOR_PACKAGE, Level.DEBUG);

        try (var proxyProducer = clientFactory.gatewayProducer()) {
            var metadataRecord = proxyProducer.send(new ProducerRecord<>(clientTopic, "key", "value"))
                    .get(5, TimeUnit.SECONDS);
            assertTrue(metadataRecord.hasOffset());
        }

        try (var proxyConsumer = clientFactory.gatewayConsumer("someGroup")) {
            proxyConsumer.subscribe(Collections.singletonList(clientTopic));
            var startTime = System.currentTimeMillis();
            var recordCount = 0;
            while (recordCount < 1 && System.currentTimeMillis() < startTime + 5_000L) {
                var records = proxyConsumer.poll(Duration.ofMillis(100));
                recordCount += records.count();
            }
            assertThat(recordCount).isEqualTo(0);
        }

        logWriter.flush();
        // global interceptor
        assertThat(logStream.toString()
                .contains("Hello there, a class org.apache.kafka.common.requests"))
                .isEqualTo(true);
        // fetch interceptor
        assertThat(logStream.toString()
                .contains("Fetch was requested from localhost"))
                .isEqualTo(false);
        assertThat(logStream.toString()
                .contains("Fetch from client localhost was responded to"))
                .isEqualTo(false);
    }

    void addAppender(final Writer writer, final String writerName) {
        final LoggerContext context = LoggerContext.getContext(false);
        final Configuration config = context.getConfiguration();
        final PatternLayout layout = PatternLayout.createDefaultLayout(config);
        final Appender appender = WriterAppender.createAppender(layout, null, writer, writerName, false, true);
        appender.start();
        config.addAppender(appender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(appender, null, null);
        }
        config.getRootLogger().addAppender(appender, null, null);
    }

    private String getGroup() {
        return "someGroup" + UUID.randomUUID();
    }

    @Override
    protected AbstractModule mockGuiceModule() {
        return new MockModule(getGatewayConfiguration());
    }

    @AllArgsConstructor
    class MockModule extends AbstractModule {

        private GatewayConfiguration configuration;

        @Provides
        @Singleton
        InterceptorPoolService interceptorPoolService() {
            var interceptorPoolService = new InterceptorPoolService(configuration);
            mockInterceptorPoolService = Mockito.spy(interceptorPoolService);
            return mockInterceptorPoolService;
        }
    }
}
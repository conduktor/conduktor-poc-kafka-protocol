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

import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class LoggerInterceptorPluginIntegrationTest extends BaseGatewayIntegrationTest {

    public static final String TEST_CLIENT_001 = "testClient001";
    public static final String SOME_KEY = "someKey";
    public static final String SOME_VALUE = "someValue";
    public static final String LOGGERINTERCEPTOR_PACKAGE = "io.conduktor.example.loggerinterceptor";

    @Test
    public void testLoadsInterceptors() {

        assertThat(getInterceptorPoolService().getAllInterceptors(ProduceRequest.class).size())
                .isEqualTo(2);
        assertThat(getInterceptorPoolService().getAllInterceptors(FetchResponse.class).size())
                .isEqualTo(3);
        assertThat(getInterceptorPoolService().getAllInterceptors(AbstractRequestResponse.class).size())
                .isEqualTo(1);
        assertThat(getInterceptorPoolService().getAllInterceptors(AbstractResponse.class).size())
                .isEqualTo(2);
    }

    @Test
    public void testInterceptsProduceMessages() throws ExecutionException, InterruptedException {

        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);

        var logStream = new ByteArrayOutputStream();
        var logWriter = new PrintWriter(logStream);
        addAppender(logWriter, "logWriter");
        Configurator.setLevel(LOGGERINTERCEPTOR_PACKAGE, Level.DEBUG);

        try (var gatewayProducer = clientFactory.gatewayProducer()) {
            ProducerRecord<String, String> record1 = new ProducerRecord(clientTopic, SOME_KEY, SOME_VALUE);
            gatewayProducer.send(record1).get();
        }

        logWriter.flush();
        // global interceptor
        assertThat(new String(logStream.toByteArray())
                .contains("Hello there, a class org.apache.kafka.common.requests.ProduceResponse was sent/received"))
                .isEqualTo(true);
        // response interceptor
        assertThat(new String(logStream.toByteArray())
                .contains("A RESPONSE was sent"))
                .isEqualTo(true);
        // produce interceptor
        assertThat(new String(logStream.toByteArray())
                .contains("Produce was called with version: " + ApiKeys.PRODUCE.latestVersion()))
                .isEqualTo(true);
    }

    @Test
    public void testInterceptsFetchMessages() throws ExecutionException, InterruptedException {

        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);

        var logStream = new ByteArrayOutputStream();
        var logWriter = new PrintWriter(logStream);
        addAppender(logWriter, "logWriter");
        Configurator.setLevel(LOGGERINTERCEPTOR_PACKAGE, Level.DEBUG);

        try (var gatewayConsumer = clientFactory.gatewayConsumer(getGroup())) {
            gatewayConsumer.subscribe(Collections.singletonList(clientTopic));
            IntStream.range(0,100).forEach(index ->
            gatewayConsumer.poll(Duration.ofMillis(100)));
        }

        logWriter.flush();
        // global interceptor
        assertThat(new String(logStream.toByteArray())
                .contains("Hello there, a class org.apache.kafka.common.requests.LeaveGroupResponse was sent/received"))
                .isEqualTo(true);
        // response interceptor
        assertThat(new String(logStream.toByteArray())
                .contains("A RESPONSE was sent"))
                .isEqualTo(true);
        // fetch interceptor
        assertThat(new String(logStream.toByteArray())
                .contains("Fetch was requested from localhost"))
                .isEqualTo(true);
        assertThat(new String(logStream.toByteArray())
                .contains("Fetch from client localhost was responded to"))
                .isEqualTo(true);
    }

    @Test
    public void addsHeaderOnProduce() throws ExecutionException, InterruptedException {
        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);

        clientFactory.addGatewayPropertyOverride(ProducerConfig.CLIENT_ID_CONFIG, TEST_CLIENT_001);
        try (var gatewayProducer = clientFactory.gatewayProducer()) {
            ProducerRecord<String, String> record1 = new ProducerRecord(clientTopic, SOME_KEY, SOME_VALUE);
            gatewayProducer.send(record1).get();
        }

        var consumedRecords = new ArrayList<ConsumerRecord>();
        try (var gatewayConsumer = clientFactory.gatewayConsumer(getGroup())) {
            gatewayConsumer.subscribe(Collections.singletonList(clientTopic));
            var startTime = System.currentTimeMillis();
            while (consumedRecords.size() == 0 && System.currentTimeMillis() < startTime + 20_000L) {
                var records = gatewayConsumer.poll(Duration.ofMillis(100));
                records.forEach(record -> consumedRecords.add(record));
            }
        }

        assertThat(consumedRecords.size())
                .isEqualTo(1);
        assertThat(consumedRecords.get(0).headers().lastHeader("fromClient").value())
                .isEqualTo(TEST_CLIENT_001.getBytes());

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

}
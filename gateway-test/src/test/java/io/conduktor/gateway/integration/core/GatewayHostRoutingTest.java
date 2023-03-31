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

package io.conduktor.gateway.integration.core;

import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.ClientFactory;
import io.conduktor.gateway.integration.util.PortHelper;
import io.conduktor.gateway.integration.util.SNIKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static io.conduktor.gateway.integration.util.CustomHostResolverUtils.injectCustomHostResolver;
import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
public class GatewayHostRoutingTest extends BaseGatewayIntegrationTest {

    public static final int MAX_KEY = 10;
    private static final int GATEWAY_PORT = PortHelper.getFreePort();

    @Test
    public void testAuthenticate_produce() throws ExecutionException, InterruptedException {
        var clientTopic = generateTopics();

        var keysToSend = IntStream.range(0, MAX_KEY).mapToObj(i -> "key" + i)
                .collect(toSet());
        try (var producer = clientFactory.gatewayProducer()) {
            injectCustomHostResolver(producer);
            keysToSend.forEach(key -> {
                try {
                    var recordMetadata = producer.send(new ProducerRecord<>(clientTopic, key, "value")).get(10, SECONDS);
                    assertThat(recordMetadata.hasOffset()).isTrue();
                } catch (final InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception exception) {
            Assertions.fail("expect no exception");
        }
    }



    @Test
    public void testAuthenticate_consume() throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        var clientTopic = generateTopics();
        var groupId = UUID.randomUUID().toString();
        var keysToSend = IntStream.range(0, MAX_KEY).mapToObj(i -> "key" + i)
                .collect(toSet());
        try (var producer = new SNIKafkaProducer<>(clientFactory.getGatewayProperties())) {
            keysToSend.forEach(key -> {
                try {
                    var recordMetadata = producer.send(new ProducerRecord<>(clientTopic, 0, key, "value")).get(10, SECONDS);
                    assertThat(recordMetadata.hasOffset()).isTrue();
                } catch (final InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        try (var consumer = clientFactory.gatewayConsumer(groupId)) {
            injectCustomHostResolver(consumer);
            var assignment = singletonList(new TopicPartition(clientTopic, 0));
            consumer.assign(assignment);
            consumer.seekToBeginning(assignment);
            var consumedKeys = new HashSet<>();
            await().atMost(10, SECONDS).untilAsserted(() -> {
                var records = consumer.poll(ofMillis(1000));
                stream(records.spliterator(), false).forEach(r -> consumedKeys.add(r.key()));
                assertThat(consumedKeys.size())
                        .isGreaterThanOrEqualTo(MAX_KEY);
            });
        }
    }

    @Override
    protected AuthenticatorType getAuthenticatorType() {
        return AuthenticatorType.SSL;
    }

    @Override
    protected int getGatewayPort() {
        return GATEWAY_PORT;
    }

    @Override
    protected void reconfigureGateway(GatewayConfiguration gatewayConfiguration) {
        gatewayConfiguration.setRouting("host");
        gatewayConfiguration.getHostPortConfiguration().setGatewayHost("tenantSNI.proxy.conduktor.local");
    }

    @Override
    protected void reconfigureClientFactory(ClientFactory clientFactory) {
        clientFactory.addGatewayPropertyOverride("bootstrap.servers", "localhost:" + GATEWAY_PORT);
        clientFactory.addGatewayPropertyOverride("security.protocol", "SSL");
        clientFactory.addGatewayPropertyOverride("sasl.mechanism", "PLAIN");
        clientFactory.addGatewayPropertyOverride("ssl.truststore.location", "config/san/truststore.jks");
        clientFactory.addGatewayPropertyOverride("ssl.truststore.password", "changeit");
        clientFactory.addGatewayPropertyOverride("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientFactory.addGatewayPropertyOverride("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientFactory.addGatewayPropertyOverride("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientFactory.addGatewayPropertyOverride("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientFactory.addGatewayPropertyOverride(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientFactory.addGatewayPropertyOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    protected String generateTopics() throws ExecutionException, InterruptedException {
        // create some virtual topics
        return createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);
    }


}


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

import io.conduktor.gateway.config.kafka.KafkaSelectorConfig;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.AwaitAssertUtils;
import io.conduktor.gateway.integration.util.ClientFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class GatewayMultipleBootstrapServersTest extends BaseGatewayIntegrationTest {

    @Override
    protected void reconfigureClientFactory(ClientFactory clientFactory) {
        super.reconfigureClientFactory(clientFactory);
    }

    @Override
    public KafkaSelectorConfig generateKafkaConfig() {
        return configAsTempFile("bootstrap.servers=localhost:" + getKafka1Port() + ",fake-host:9092,fake-host-2:9092");
    }

    @Test
    public void testMultipleBootstrapServers() throws ExecutionException, InterruptedException {
        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);
        AwaitAssertUtils.awaitUntilAsserted(20, () -> {
            try (var gatewayProducer = clientFactory.gatewayProducer()) {
                ProducerRecord<String, String> record1 = new ProducerRecord<>(clientTopic, "key", "value");

                var recordMetadata = gatewayProducer.send(record1).get();

                assertThat(recordMetadata.hasOffset())
                        .isTrue();
            }
        });
        // verify records in Kafka
        try (var kafkaConsumer = clientFactory.kafkaConsumer("someGroup")) {
            kafkaConsumer.assign(Collections.singletonList(new TopicPartition(clientTopic, 0)));
            long startTime = System.currentTimeMillis();
            int recordCount = 0;
            ConsumerRecords<String, String> records = null;
            while (recordCount < 1 && System.currentTimeMillis() < startTime + 10000L) {
                records = kafkaConsumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                recordCount += records.count();
            }

            assertThat(records).isNotNull();
            assertThat(records.count()).isEqualTo(1);
            var record = records.iterator().next();
            assertThat(record.key()).isEqualTo("key");
            assertThat(record.value())
                    .isEqualTo("value");
        }
    }
}

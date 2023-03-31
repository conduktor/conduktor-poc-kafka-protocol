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

package io.conduktor.gateway.integration.concurrency;

import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.ClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class GatewayConcurrencyTest extends BaseGatewayIntegrationTest {

    @Test
    public void multiAdmin() throws Exception {
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();
        try (var producer = clientFactory.gatewayProducer()) {
            for (var i = 0; i < 1; i++) {
                var recordMetadata = producer.send(new ProducerRecord<>(clientTopic1, "value", "value"))
                        .get(5, SECONDS);
                assertThat(recordMetadata.hasOffset()).isTrue();
            }
        }
        var clientTopic2 = pairOfClientTopic.getRight();
        try (var producer = clientFactory.gatewayProducer()) {
            for (var i = 0; i < 1; i++) {
                var recordMetadata = producer.send(new ProducerRecord<>(clientTopic2, "value", "value"))
                        .get(5, SECONDS);
                assertThat(recordMetadata.hasOffset()).isTrue();
            }
        }
        try (var consumer = clientFactory.gatewayConsumer("groupId")) {
            consumer.subscribe(List.of(clientTopic1, clientTopic2));
            int recordCount = 0;
            long startTime = System.currentTimeMillis();
            while (recordCount < 2 && System.currentTimeMillis() < startTime + 10000L) {
                var records = consumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                recordCount += records.count();
                for (var record : records) {
                    assertThat(record.value()).contains("value");
                }
            }
            assertThat(recordCount).isEqualTo(2);
        }
    }

    @Test
    public void multiProducers() throws Exception {
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();
        try (var producer = clientFactory.gatewayProducer()) {
            for (var i = 0; i < 1; i++) {
                var recordMetadata = producer.send(new ProducerRecord<>(clientTopic1, "value", "value"))
                        .get(5, SECONDS);
                assertThat(recordMetadata.hasOffset()).isTrue();
            }
        }
        var clientTopic2 = pairOfClientTopic.getRight();
        try (var producer = clientFactory.gatewayProducer()) {
            for (var i = 0; i < 1; i++) {
                var recordMetadata = producer.send(new ProducerRecord<>(clientTopic2, "value", "value"))
                        .get(5, SECONDS);
                assertThat(recordMetadata.hasOffset()).isTrue();
            }
        }
        clientFactory.addGatewayPropertyOverride("max.poll.records", "1");
        try (var consumer = clientFactory.gatewayConsumer("groupId")) {
            consumer.subscribe(List.of(clientTopic1, clientTopic2));
            var startTime = System.currentTimeMillis();
            var recordCounter = 0;
            while (recordCounter < 2 && System.currentTimeMillis() < startTime + 10000L) {
                var records = consumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                recordCounter += records.count();
            }
            assertThat(recordCounter).isEqualTo(2);
        }
    }

    @Test
    public void testMultipleProducerAndSameGatewayTopic() throws ExecutionException, InterruptedException, TimeoutException {
        var executorService = Executors.newFixedThreadPool(2);
        var clientTopic = generateTopics().getLeft();
        var produceFuture = executorService.submit(() -> produce(clientTopic, "producer1", 2));
        var produceFuture2 = executorService.submit(() -> produce(clientTopic, "producer2", 3));

        produceFuture.get(60, SECONDS);
        produceFuture2.get(60, SECONDS);
        executorService.shutdownNow();
    }

    @Test
    public void testMultipleProducerAndMultipleGatewayTopic() throws ExecutionException, InterruptedException, TimeoutException {
        var executorService = Executors.newFixedThreadPool(2);
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();
        var clientTopic2 = pairOfClientTopic.getRight();
        var produceFuture = executorService.submit(() -> produce(clientTopic1, "producer1", 3));
        var produceFuture2 = executorService.submit(() -> produce(clientTopic2, "producer2", 2));

        produceFuture.get(60, SECONDS);
        produceFuture2.get(60, SECONDS);
        executorService.shutdownNow();
    }

    @Test
    public void testMultipleConsumerAndSameGatewayTopic() throws ExecutionException, InterruptedException, TimeoutException {
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();

        var maxNumberOfRecords = 5;
        var prefixValue = "prefix_";
        produce(clientTopic1, prefixValue, maxNumberOfRecords);
        var executorService = Executors.newFixedThreadPool(2);
        var consumeFuture = executorService.submit(() -> consume("consumeGroup1", clientTopic1, prefixValue, maxNumberOfRecords));
        var consumeFuture2 = executorService.submit(() -> consume("consumeGroup2", clientTopic1, prefixValue, maxNumberOfRecords));

        var recordCount = consumeFuture.get(60, SECONDS);
        var recordCount2 = consumeFuture2.get(60, SECONDS);
        assertThat(recordCount + recordCount2).isEqualTo(maxNumberOfRecords * 2);
        executorService.shutdownNow();
    }

    @Test
    public void testMultipleConsumerAndMultipleGatewayTopic() throws ExecutionException, InterruptedException, TimeoutException {
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();
        var clientTopic2 = pairOfClientTopic.getRight();

        var maxNumberOfRecords = 1;
        var maxNumberOfRecords2 = 3;
        var prefixValue = "prefix_1_";
        var prefixValue2 = "prefix_2_";
        produce(clientTopic1, prefixValue, maxNumberOfRecords);
        produce(clientTopic2, prefixValue2, maxNumberOfRecords2);

        var executorService = Executors.newFixedThreadPool(2);
        var consumeFuture = executorService.submit(() -> consume("consumeGroup1", clientTopic1, prefixValue, maxNumberOfRecords));
        var consumeFuture2 = executorService.submit(() -> consume("consumeGroup2", clientTopic2, prefixValue2, maxNumberOfRecords2));

        var recordCount = consumeFuture.get(200, SECONDS);
        var recordCount2 = consumeFuture2.get(200, SECONDS);
        assertThat(recordCount).isEqualTo(maxNumberOfRecords);
        assertThat(recordCount2).isEqualTo(maxNumberOfRecords2);
        executorService.shutdownNow();
    }

    @Test
    public void testFetchMultiBatchShouldReturnControlBatches() throws ExecutionException, InterruptedException {
        //create client topic 1 +  client topic 2
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();
        var clientTopic2 = pairOfClientTopic.getRight();

        //produce large number of records for client topic 1, and 1 message for client topic 2
        //records of client topic 1: offset 0 -> 99
        //records of client topic 2: offset 100
        var maxNumberOfRecords = 100;
        var maxNumberOfRecords2 = 1;

        produce(clientTopic1, "value_01", maxNumberOfRecords);
        produce(clientTopic2, "value_02", maxNumberOfRecords2);

        //fetch.max.bytes only 50 -> records will be returned multiple times
        clientFactory.addGatewayPropertyOverride("fetch.max.bytes", "50");
        var recordCount = consume("consumeGroup1", clientTopic1, "value_01", maxNumberOfRecords);

        var recordCount2 = consume("consumeGroup2", clientTopic2, "value_02", maxNumberOfRecords2);
        assertThat(recordCount).isEqualTo(maxNumberOfRecords);
        assertThat(recordCount2).isEqualTo(maxNumberOfRecords2);
    }

    @Test
    public void testMultipleProducerAndConsumerWithSameGatewayTopic() throws ExecutionException, InterruptedException, TimeoutException {
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();

        var executorService = Executors.newFixedThreadPool(4);
        var maxNumberOfRecords = 3;
        var maxNumberOfRecords2 = 4;
        var prefix = "prefix";
        var prefixValue = prefix + "_1_";
        var prefixValue2 = prefix + "_2_";
        var produceFuture = executorService.submit(() -> produce(clientTopic1, prefixValue, maxNumberOfRecords));
        var produceFuture2 = executorService.submit(() -> produce(clientTopic1, prefixValue2, maxNumberOfRecords2));
        var consumeFuture = executorService.submit(() -> consume("consumeGroup1", clientTopic1, prefix, (maxNumberOfRecords + maxNumberOfRecords2)));
        var consumeFuture2 = executorService.submit(() -> consume("consumeGroup1", clientTopic1, prefix, (maxNumberOfRecords + maxNumberOfRecords2)));

        produceFuture.get(120, SECONDS);
        produceFuture2.get(120, SECONDS);
        var recordCount = consumeFuture.get(300, SECONDS);
        var recordCount2 = consumeFuture2.get(300, SECONDS);
        assertThat(recordCount + recordCount2).isEqualTo((maxNumberOfRecords + maxNumberOfRecords2) * 2);
        executorService.shutdownNow();
    }

    @Test
    public void testMultipleProducerAndConsumerWithMultipleGatewayTopic() throws ExecutionException, InterruptedException, TimeoutException {
        var pairOfClientTopic = generateTopics();
        var clientTopic1 = pairOfClientTopic.getLeft();
        var clientTopic2 = pairOfClientTopic.getRight();

        var executorService = Executors.newFixedThreadPool(4);
        var maxNumberOfRecords = 2;
        var maxNumberOfRecords2 = 4;
        var prefixValue = "prefix_1_";
        var prefixValue2 = "prefix_2_";
        var produceFuture = executorService.submit(() -> produce(clientTopic1, prefixValue, maxNumberOfRecords));
        var produceFuture2 = executorService.submit(() -> produce(clientTopic2, prefixValue2, maxNumberOfRecords2));
        var consumeFuture = executorService.submit(() -> consume("consumeGroup1", clientTopic1, prefixValue, maxNumberOfRecords));
        var consumeFuture2 = executorService.submit(() -> consume("consumeGroup2", clientTopic2, prefixValue2, maxNumberOfRecords2));

        produceFuture.get(60, SECONDS);
        produceFuture2.get(60, SECONDS);
        var recordCount = consumeFuture.get(120, SECONDS);
        var recordCount2 = consumeFuture2.get(120, SECONDS);
        assertThat(recordCount).isEqualTo(maxNumberOfRecords);
        assertThat(recordCount2).isEqualTo(maxNumberOfRecords2);
        executorService.shutdownNow();
    }

    @Override
    protected void reconfigureClientFactory(ClientFactory clientFactory) {
        super.reconfigureClientFactory(clientFactory);
        clientFactory.addGatewayPropertyOverride(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientFactory.addGatewayPropertyOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    private void produce(String topic, String prefixValue, int maxNumberOfRecords) {
        try (var producer = clientFactory.gatewayProducer()) {
            for (var i = 0; i < maxNumberOfRecords; i++) {
                var value = prefixValue + "_value_" + i;
                var record = new ProducerRecord<>(topic, "key", value);
                var recordMetadata = producer.send(record).get(10, SECONDS);
                assertTrue(recordMetadata.hasOffset());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private int consume(String groupId, String topic, String prefixValue, int maxNumberOfRecords) {
        try (var consumer = clientFactory.gatewayConsumer(groupId)) {
            consumer.assign(singletonList(new TopicPartition(topic, 0)));
            consumer.seekToBeginning(singletonList(new TopicPartition(topic, 0)));
            int recordCount = 0;
            long startTime = System.currentTimeMillis();
            while (recordCount < maxNumberOfRecords && System.currentTimeMillis() < startTime + 10000L) {
                var records = consumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                recordCount += records.count();
                for (var record : records) {
                    assertThat(record.value()).contains(prefixValue);
                }
            }
            return recordCount;
        }
    }

    private Pair<String, String> generateTopics() throws ExecutionException, InterruptedException {
        // create some virtual topics
        var clientTopicName1 = "test_topic_" + UUID.randomUUID();
        var clientTopicName2 = "test_topic_" + UUID.randomUUID();
        createTopic(clientFactory.kafkaAdmin(), clientTopicName1, 1, (short) 1);
        createTopic(clientFactory.kafkaAdmin(), clientTopicName2, 1, (short) 1);
        return Pair.of(clientTopicName1, clientTopicName2);
    }

}

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

package io.conduktor.gateway.integration.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class ProduceConsumeUtils {

    public static void produceAndVerify(KafkaProducer<String, String> producer, String clientTopic, int numRecords) {
        produceAndVerify(producer, clientTopic, "", numRecords);
    }

    public static void produceAndVerify(KafkaProducer<String, String> producer, String clientTopic, String value, int numRecords) {
        var keysToSend = IntStream.range(0, numRecords).sorted().mapToObj(i -> "Key" + i)
                .toList();
        keysToSend.forEach(key -> {
            try {
                var recordValue = StringUtils.isBlank(value) ? "value" : value;
                var recordMetadata = producer.send(new ProducerRecord<>(clientTopic, key, recordValue))
                        .get(30, SECONDS);
                assertTrue(recordMetadata.hasOffset());
            } catch (final InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void consumeAndVerify(KafkaConsumer<String, String> consumer, String clientTopic, int numRecords) {
        consumeAndVerify(consumer, List.of(clientTopic), "", numRecords);
    }

    public static void consumeAndVerify(KafkaConsumer<String, String> consumer, String clientTopic, String recordValue, int numRecords) {
        consumeAndVerify(consumer, List.of(clientTopic), recordValue, numRecords);
    }

    public static void consumeAndVerify(KafkaConsumer<String, String> consumer, List<String> clientTopics, String value, int numRecords) {
        consumer.subscribe(clientTopics);
        var consumedKeys = new ArrayList<>();
        var startTime = System.currentTimeMillis();
        while (consumedKeys.size() < numRecords && System.currentTimeMillis() < startTime + 10000L) {
            var records = consumer.poll(ofMillis(100));
            if (records.isEmpty()) {
                continue;
            } else {
                log.warn("record from server: {} is not empty: {}", records, records.isEmpty());
            }
            stream(records.spliterator(), false).forEach(r -> {
                consumedKeys.add(r.key());
                var recordValue = StringUtils.isBlank(value) ? "value" : value;
                Assertions.assertThat(r.value()).isEqualTo(recordValue);

            });
            if (consumedKeys.size() == numRecords) {
                break;
            }
        }
        assertThat(consumedKeys).hasSize(numRecords);
    }
}

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

package io.conduktor.gateway.integration.error.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GatewayConsumerErrorHandlerTest extends BaseErrorHandlerTest {

    private static Stream<Arguments> consumerTestArguments() {
        return Stream.of(
                Arguments.of(ApiKeys.FETCH, ExceptionMockType.REQUEST, null), //retry
                Arguments.of(ApiKeys.FETCH, ExceptionMockType.RESPONSE, null), //retry
                Arguments.of(ApiKeys.FIND_COORDINATOR, ExceptionMockType.REQUEST, UnknownServerException.class),
                Arguments.of(ApiKeys.FIND_COORDINATOR, ExceptionMockType.RESPONSE, UnknownServerException.class),
                Arguments.of(ApiKeys.LIST_OFFSETS, ExceptionMockType.REQUEST, UnknownServerException.class),
                Arguments.of(ApiKeys.LIST_OFFSETS, ExceptionMockType.RESPONSE, UnknownServerException.class)
        );
    }

    //need better way to verify exception
    /**
     * when exceptedException = null is equivalent to consumer will continue to poll even though there is an error
     */
    @ParameterizedTest
    @MethodSource("consumerTestArguments")
    @Tag("IntegrationTest")
    public void testConsume(ApiKeys apiKeys, ExceptionMockType type, Class<?> expectedException) throws Exception {
        var gatewayTopic = generateClientTopic();
        try (var gatewayProducer = clientFactory.gatewayProducer()) {
            var record = new ProducerRecord<>(gatewayTopic, KEY_1, VAL_1);
            var recordMetadata = gatewayProducer.send(record).get(3, TimeUnit.SECONDS);
            assertThat(recordMetadata.hasOffset()).isTrue();
        }
        mockException(type, apiKeys);
        try (var gatewayConsumer = clientFactory.gatewayConsumer("someGroupId")) {
            gatewayConsumer.assign(singletonList(new TopicPartition(gatewayTopic, 0)));
            if (Objects.isNull(expectedException)) {
                var records = gatewayConsumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                assertThat(records).hasSize(0);
            } else if (expectedException.isAssignableFrom(TimeoutException.class)) {
                assertThatThrownBy(() -> gatewayConsumer.poll(Duration.of(2, ChronoUnit.SECONDS)))
                        .cause()
                        .isInstanceOf(TimeoutException.class);
            } else {
                assertThatThrownBy(() -> gatewayConsumer.poll(Duration.of(2, ChronoUnit.SECONDS)))
                        .isInstanceOf(expectedException);
            }
        }
    }
}

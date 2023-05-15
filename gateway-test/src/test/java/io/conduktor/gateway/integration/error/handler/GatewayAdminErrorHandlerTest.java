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

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GatewayAdminErrorHandlerTest extends BaseErrorHandlerTest {

    @ParameterizedTest
    @EnumSource(ExceptionMockType.class)
    @Tag("IntegrationTest")
    public void testCreateTopics(ExceptionMockType type) throws ExecutionException, InterruptedException {
        mockException(type, ApiKeys.CREATE_TOPICS);
        createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);
        try (var gatewayAdmin = clientFactory.gatewayAdmin()) {
            var newTopics = new NewTopic("new_topic", 1, (short) 1);
            assertThatThrownBy(() -> gatewayAdmin.createTopics(List.of(newTopics)).all().get())
                    .cause()
                    .isInstanceOf(UnknownServerException.class);
        }
    }

    @ParameterizedTest
    @EnumSource(ExceptionMockType.class)
    @Tag("IntegrationTest")
    public void testAlterConfigs(ExceptionMockType type) throws ExecutionException, InterruptedException {
        mockException(type, ApiKeys.ALTER_CONFIGS);
        var gatewayTopic = generateClientTopic();
        try (var gatewayAdmin = clientFactory.gatewayAdmin()) {
            var map = Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, gatewayTopic),
                    new Config(List.of(new ConfigEntry("max.block.ms", "5000")))
            );
            assertThatThrownBy(() -> gatewayAdmin.alterConfigs(map).all().get())
                    .cause()
                    .isInstanceOf(UnknownServerException.class);
        }
    }

    @ParameterizedTest
    @EnumSource(ExceptionMockType.class)
    @Tag("IntegrationTest")
    public void testIncrementalAlterConfigs(ExceptionMockType type) throws ExecutionException, InterruptedException {
        mockException(type, ApiKeys.INCREMENTAL_ALTER_CONFIGS);
        var gatewayTopic = generateClientTopic();
        try (var gatewayAdmin = clientFactory.gatewayAdmin()) {
            var map = Map.<ConfigResource, Collection<AlterConfigOp>>of(
                    new ConfigResource(ConfigResource.Type.TOPIC, gatewayTopic),
                    List.of(new AlterConfigOp(new ConfigEntry("max.block.ms", "5000"), AlterConfigOp.OpType.SET))
            );
            assertThatThrownBy(() -> gatewayAdmin.incrementalAlterConfigs(map).all().get())
                    .cause()
                    .isInstanceOf(UnknownServerException.class);
        }
    }

    @ParameterizedTest
    @EnumSource(ExceptionMockType.class)
    @Tag("IntegrationTest")
    public void testDeleteTopics(ExceptionMockType type) throws ExecutionException, InterruptedException {
        mockException(type, ApiKeys.DELETE_TOPICS);
        var gatewayTopic = generateClientTopic();
        try (var gatewayAdmin = clientFactory.gatewayAdmin()) {
            assertThatThrownBy(() -> gatewayAdmin.deleteTopics(List.of(gatewayTopic)).all().get())
                    .cause()
                    .isInstanceOf(UnknownServerException.class);
        }
    }

    @ParameterizedTest
    @EnumSource(ExceptionMockType.class)
    @Tag("IntegrationTest")
    public void testDescribeConfigs(ExceptionMockType type) throws ExecutionException, InterruptedException {
        mockException(type, ApiKeys.DESCRIBE_CONFIGS);
        var gatewayTopic = generateClientTopic();
        try (var gatewayAdmin = clientFactory.gatewayAdmin()) {
            assertThatThrownBy(() -> gatewayAdmin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, gatewayTopic))).all().get())
                    .cause()
                    .isInstanceOf(UnknownServerException.class);
        }
    }

}

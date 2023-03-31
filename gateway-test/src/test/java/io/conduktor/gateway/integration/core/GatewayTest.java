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

import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.ProduceConsumeUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class GatewayTest extends BaseGatewayIntegrationTest {

    public static final String VAL_1 = "val1";


    @Test
    public void testCanProduceThroughGateway() throws ExecutionException, InterruptedException {

        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);
        ProduceConsumeUtils.produceAndVerify(clientFactory.gatewayProducer(), clientTopic, VAL_1, 1);

        // verify records in Kafka
        ProduceConsumeUtils.consumeAndVerify(clientFactory.kafkaConsumer("someGroupId"), clientTopic, VAL_1, 1);
    }

    @Test
    public void testDescribeNonTopicConfigsNoOp() throws ExecutionException, InterruptedException {
        // this is internal playground functionality and so not subject to early version support
        // this should be disabled for early brokers
        if (shouldTest()) {
            return;
        }

        try (var gatewayAdminClient = clientFactory.gatewayAdmin()) {
            var brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
            var result = gatewayAdminClient.describeConfigs(Collections.singletonList(brokerResource)).all().get();
            assertThat(result.get(brokerResource).entries().size())
                    .isEqualTo(0);
        }
    }

    @Test
    public void testAlterNonTopicConfigsNoOp() throws ExecutionException, InterruptedException {
        // this is internal playground functionality and so not subject to early version support
        // this should be disabled for early brokers
        if (shouldTest()) {
            return;
        }

        var brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        try (var gatewayAdminClient = clientFactory.gatewayAdmin()) {
            var configs = Map.of(brokerResource,
                    new Config(Collections.singletonList(new ConfigEntry("log.cleaner.threads", "200"))));
            gatewayAdminClient.alterConfigs(configs).all().get();
        }

        try (var kafkaAdminClient = clientFactory.kafkaAdmin()) {
            var brokerConfigs = kafkaAdminClient.describeConfigs(Collections.singletonList(brokerResource)).all().get();
            assertThat(brokerConfigs.get(brokerResource).get("log.cleaner.threads").value())
                    .isEqualTo("1");
        }
    }

    @Test
    public void testAlterNonTopicConfigsIncrementalNoOp() throws ExecutionException, InterruptedException {
        // this is internal playground functionality and so not subject to early version support
        // this should be disabled for early brokers
        if (shouldTest()) {
            return;
        }

        var brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        try (var gatewayAdminClient = clientFactory.gatewayAdmin()) {
            Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(brokerResource,
                    Collections.singletonList(new AlterConfigOp(
                            new ConfigEntry("log.cleaner.threads", "200"),
                            AlterConfigOp.OpType.SET)));

            gatewayAdminClient.incrementalAlterConfigs(configs).all().get();
        }

        try (var kafkaAdminClient = clientFactory.kafkaAdmin()) {
            var brokerConfigs = kafkaAdminClient.describeConfigs(Collections.singletonList(brokerResource)).all().get();
            assertThat(brokerConfigs.get(brokerResource).get("log.cleaner.threads").value())
                    .isEqualTo("1");
        }
    }

    private boolean shouldTest() {
        return (System.getenv() == null || System.getenv("CP_VERSION") == null || System.getenv("CP_VERSION").isBlank())
                || !System.getenv("CP_VERSION").equals("latest");
    }

}

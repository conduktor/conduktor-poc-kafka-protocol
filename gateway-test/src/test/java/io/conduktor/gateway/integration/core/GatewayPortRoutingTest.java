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

import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.ProduceConsumeUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

@Slf4j
public class GatewayPortRoutingTest extends BaseGatewayIntegrationTest {
    public static final int MAX_KEY = 10;

    @Test
    public void testProduceAndConsume() throws ExecutionException, InterruptedException {
        var clientTopic = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);
        ProduceConsumeUtils.produceAndVerify(clientFactory.gatewayProducer(), clientTopic, MAX_KEY);
        ProduceConsumeUtils.consumeAndVerify(clientFactory.gatewayConsumer("someGroupId"), clientTopic, MAX_KEY);
    }

    @Override
    protected void reconfigureGateway(GatewayConfiguration gatewayConfiguration) {
        gatewayConfiguration.getHostPortConfiguration().setGatewayHost("localhost");
    }

}

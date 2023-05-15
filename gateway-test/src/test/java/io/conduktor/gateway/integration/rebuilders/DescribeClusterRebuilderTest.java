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

package io.conduktor.gateway.integration.rebuilders;

import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DescribeClusterRebuilderTest extends BaseGatewayIntegrationTest {


    @Test
    @Tag("IntegrationTest")
    public void testShouldReturnGatewayHostNames() throws ExecutionException, InterruptedException {
        try (var admin = clientFactory.gatewayAdmin()) {
            var brokers = admin.describeCluster().nodes().get();
            assertThat(brokers)
                    .hasSize(2);
            var minPort = Integer.parseInt(getGatewayConfiguration().getHostPortConfiguration().getPortRange().split(":")[0]);
            var maxPort = Integer.parseInt(getGatewayConfiguration().getHostPortConfiguration().getPortRange().split(":")[1]);
            assertThat(brokers.stream().max(Comparator.comparingInt(Node::port)).get().port())
                    .isLessThanOrEqualTo(maxPort);
            assertThat(brokers.stream().min(Comparator.comparingInt(Node::port)).get().port())
                    .isGreaterThanOrEqualTo(minPort);
        }
    }

}

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

package io.conduktor.gateway.rebuilder.components;

import io.conduktor.gateway.config.Endpoint;
import io.conduktor.gateway.service.ClientRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.conduktor.gateway.common.NodeUtils.keyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class DescribeClusterReBuilderTest extends ComponentBaseTest {

    @Test
    public void testMasksHostNames() {

        var response = getDescribeClusterResponseTemplate();

        var requestHeader = getRequestHeader();

        var clientRequest = new ClientRequest(requestHeader, null, "someNode");
        clientRequest.initGatewayRequestHeader(requestHeader.correlationId());

        var firstBroker = response.data().brokers().stream().findFirst().get();
        var mappingKey = keyOf(firstBroker.host(), firstBroker.port());
        when(brokerManager.getRealToGatewayMap(response.data().brokers())).thenReturn(
            Map.of(mappingKey, new Endpoint("b1.proxy", firstBroker.port()))
        );

        var reBuilder = new DescribeClusterRebuilder(mockRebuilderTools);

        reBuilder.rebuildResponse(response, clientRequest);

        // drop unmapped broker
        assertThat(response.data().brokers().size())
                .isEqualTo(1);
        assertThat(response.data().brokers().iterator().next().host())
                .isEqualTo("b1.proxy");
        assertThat(response.data().brokers().iterator().next().port())
                .isEqualTo(firstBroker.port());
        assertThat(response.data().brokers().iterator().next().brokerId())
                .isEqualTo(firstBroker.brokerId());
    }
}

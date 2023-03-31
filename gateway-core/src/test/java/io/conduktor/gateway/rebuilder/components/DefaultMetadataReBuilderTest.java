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
import io.conduktor.gateway.model.User;
import io.conduktor.gateway.service.ClientRequest;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DefaultMetadataReBuilderTest extends ComponentBaseTest {

    @Test
    public void testRebuildResponse_shouldMapHosts() {

        when(brokerManager.getRealToGatewayMap(any(MetadataResponseData.MetadataResponseBrokerCollection.class))).thenReturn(Map.of(
                "kafka:9092", new Endpoint("gateway", 6969)
        ));
        var rebuilder = new DefaultMetadataReBuilder(mockRebuilderTools);
        var response = buildMetadataResponseTemplate();
        var clientRequest = Mockito.mock(ClientRequest.class);
        var user = Mockito.mock(User.class);
        rebuilder.rebuildResponse(response, clientRequest);

        assertThat(response.brokers().size())
                .isEqualTo(1);
        assertThat(response.brokers().iterator().next())
                .hasFieldOrPropertyWithValue("host", "gateway")
                .hasFieldOrPropertyWithValue("port", 6969);
    }

    private MetadataResponse buildMetadataResponseTemplate() {
        var responseData = new MetadataResponseData();
        var responseBroker = new MetadataResponseData.MetadataResponseBroker();
        responseBroker.setHost("kafka");
        responseBroker.setPort(9092);
        responseData.setBrokers(new MetadataResponseData.MetadataResponseBrokerCollection(
                Arrays.asList(responseBroker).iterator()
        ));
        return new MetadataResponse(responseData, ApiKeys.METADATA.latestVersion());
    }


}

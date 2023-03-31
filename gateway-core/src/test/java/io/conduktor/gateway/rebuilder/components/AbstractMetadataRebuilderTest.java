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

import io.conduktor.gateway.network.BrokerManager;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.RebuilderTools;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractMetadataRebuilderTest {

    private final RebuilderTools mockRebuilerTools = mock(RebuilderTools.class);
    private final BrokerManager mockBrokerManager = mock(BrokerManager.class);

    @BeforeEach
    public void setup() {
        setupMocks();
    }

    @Test
    public void testBadHostMappingDoesNotReturnKafkaHost() {

        var testRebuilder = new TestMetadataRebuilder(mockRebuilerTools);
        var response = getMetadataResponse();
        testRebuilder.rebuildHost(response);

        assertThat(response.brokers().size())
                .isEqualTo(0);
    }

    private void setupMocks() {
        when(mockRebuilerTools.brokerManager()).thenReturn(mockBrokerManager);
        when(mockBrokerManager.getRealToGatewayMap(any(MetadataResponseData.MetadataResponseBrokerCollection.class))).thenReturn(Collections.emptyMap());
    }

    private MetadataResponse getMetadataResponse() {
        var responseBroker = new MetadataResponseData.MetadataResponseBroker();
        responseBroker.setHost("kafka");
        responseBroker.setPort(9092);
        var brokerCollection = new MetadataResponseData.MetadataResponseBrokerCollection();
        brokerCollection.add(responseBroker);
        var responseData = new MetadataResponseData();
        responseData.setBrokers(brokerCollection);
        return new MetadataResponse(responseData, ApiKeys.METADATA.latestVersion());
    }

    private static class TestMetadataRebuilder extends AbstractMetadataReBuilder {

        public TestMetadataRebuilder(RebuilderTools rebuilderTools) {
            super(rebuilderTools);
        }

        @Override
        public CompletionStage<MetadataRequest> rebuildRequest(MetadataRequest request, ClientRequest clientRequest) {
            return null;
        }

        @Override
        public CompletionStage<MetadataResponse> rebuildResponse(MetadataResponse response, ClientRequest clientRequest) {
            return null;
        }

    }


}

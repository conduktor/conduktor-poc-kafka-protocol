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

import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.model.User;
import io.conduktor.gateway.network.BrokerManagerWithPortMapping;
import io.conduktor.gateway.service.ClientService;
import io.conduktor.gateway.service.RebuilderTools;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public abstract class ComponentBaseTest {


    public static final User TEST_USER = new User("ignoredUser");

    public static final String CLIENT_TOPIC_1 = "clientTopic1";
    public static final String CLIENT_TOPIC_2 = "clientTopic2";
    public static final String CLIENT_TOPIC_3 = "clientTopic3";

    public static final String CLIENT_TOPIC_COMPACTED = "compactedTopic1";
    public static final String GROUP_NAME = "someGroup";

    protected RebuilderTools mockRebuilderTools = mock(RebuilderTools.class);

    protected BrokerManagerWithPortMapping brokerManager = mock(BrokerManagerWithPortMapping.class);

    protected ClientService mockClientService = mock(ClientService.class);

    protected MetricsRegistryProvider mockMetricsRegistryProvider = mock(MetricsRegistryProvider.class);

    protected Timer mockTimer = mock(Timer.class);

    @BeforeEach
    public void setup() {
        setupMocks();
    }

    protected RequestHeader getRequestHeader() {
        var requestHeaderData = new RequestHeaderData();
        var requestHeader = new RequestHeader(requestHeaderData, ApiKeys.FETCH.latestVersion());
        return requestHeader;
    }


    protected DescribeClusterResponse getDescribeClusterResponseTemplate() {

        var brokerCollection = new DescribeClusterResponseData.DescribeClusterBrokerCollection();
        var broker1  = new DescribeClusterResponseData.DescribeClusterBroker();
        broker1.setHost("b1.upstreamHost");
        broker1.setBrokerId(1);
        broker1.setPort(9091);
        var broker2  = new DescribeClusterResponseData.DescribeClusterBroker();
        broker2.setHost("b2.upstreamHost");
        broker2.setBrokerId(2);
        broker2.setPort(9092);

        brokerCollection.add(broker1);
        brokerCollection.add(broker2);

        var responseData = new DescribeClusterResponseData();
        responseData.setBrokers(brokerCollection);

        var response = new DescribeClusterResponse(responseData);
        return response;
    }



    protected MetadataResponse getMetadataResponseTemplate() {
        var responseData = new MetadataResponseData();
        var topicCollection = new MetadataResponseData.MetadataResponseTopicCollection();
        var responseTopic = new MetadataResponseData.MetadataResponseTopic();
        var responsePartition = new MetadataResponseData.MetadataResponsePartition();
        responsePartition.setPartitionIndex(0);
        responseTopic.setName(CLIENT_TOPIC_1);
        responseTopic.setPartitions(Collections.singletonList(responsePartition));
        responseTopic.setPrev(-2);
        responseTopic.setNext(-2);
        topicCollection.add(responseTopic);
        responseData.setTopics(topicCollection);
        return new MetadataResponse(responseData, ApiKeys.METADATA.latestVersion());
    }

    protected FindCoordinatorRequest getFindCoordinatorRequest() {
        var requestData = new FindCoordinatorRequestData();
        var request = new FindCoordinatorRequest.Builder(requestData).build();
        requestData.setKey(GROUP_NAME);
        return request;
    }

    private void setupMocks() {

        when(mockRebuilderTools.brokerManager()).thenReturn(brokerManager);
        when(mockRebuilderTools.metricsRegistryProvider()).thenReturn(mockMetricsRegistryProvider);
        when(mockRebuilderTools.clientService()).thenReturn(mockClientService);
        doNothing().when(mockTimer).record(anyLong(),any());
        when(mockMetricsRegistryProvider.globalTimer()).thenReturn(mockTimer);
    }

}

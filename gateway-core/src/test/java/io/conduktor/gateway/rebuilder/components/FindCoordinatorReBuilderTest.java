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
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


public class FindCoordinatorReBuilderTest extends ComponentBaseTest {

    private static FindCoordinatorResponseData.Coordinator getCoordinator(int nodeId, String host, int port) {
        var coordinator = new FindCoordinatorResponseData.Coordinator();
        coordinator.setNodeId(nodeId);
        coordinator.setHost(host);
        coordinator.setPort(port);
        return coordinator;
    }

    @Test
    public void testFindCoordinationResponsesWhenResponseContainsBroker() throws ExecutionException, InterruptedException {
        var nodeId = 1;
        var host = "example.com";
        var port = 1234;
        var gatewayEndPointHost = "gatewayendpointhost.com";
        var endPointPort = 4321;

        when(brokerManager.getGatewayByReal(host, port)).thenReturn(
                new Endpoint(gatewayEndPointHost, endPointPort));

        var reBuilder  = new DefaultFindCoordinatorReBuilder(mockRebuilderTools);
        var response = initApiVersionResponse(nodeId, host, port);
        var rebuilt = reBuilder.rebuildResponse(response,  Mockito.mock(ClientRequest.class)).toCompletableFuture().get();
        // assert request
        var coordinators = rebuilt.data().coordinators();
        assertThat(coordinators).hasSize(1);
        assertThat(coordinators.get(0).host()).isEqualTo(gatewayEndPointHost);
        assertThat(coordinators.get(0).port()).isEqualTo(endPointPort);

    }

    @Test
    public void testFindCoordinationResponsesWhenResponseDoesNotContainBroker() throws ExecutionException, InterruptedException {
        var nodeId = 1;
        var host = "example.com";
        var port = 1234;
        var clientRequest = Mockito.mock(ClientRequest.class);

        var reBuilder  = new DefaultFindCoordinatorReBuilder(mockRebuilderTools);
        var response = initApiVersionResponse(nodeId, host, port);
        var rebuilt = reBuilder.rebuildResponse(response, clientRequest).toCompletableFuture().get();

        // assert request
        var coordinators = rebuilt.data().coordinators();

        assertThat(coordinators).hasSize(1);
        assertThat(coordinators.get(0).errorCode()).isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE.code());
        assertThat(coordinators.get(0).host()).isEqualTo("");
        assertThat(coordinators.get(0).port()).isEqualTo(0);
    }

    private FindCoordinatorResponse initApiVersionResponse(int nodeId, String host, int port) {
        var responseData = new FindCoordinatorResponseData();
        var coordinators = new ArrayList<FindCoordinatorResponseData.Coordinator>();
        coordinators.add(getCoordinator(nodeId, host, port));
        responseData.setCoordinators(coordinators);
        return new FindCoordinatorResponse(responseData);
    }
}

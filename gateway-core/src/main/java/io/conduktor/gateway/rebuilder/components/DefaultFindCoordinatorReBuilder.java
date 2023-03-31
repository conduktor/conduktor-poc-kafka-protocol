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

import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.RebuilderTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Slf4j
public class DefaultFindCoordinatorReBuilder extends AbstractFindCoordinatorReBuilder {

    public DefaultFindCoordinatorReBuilder(RebuilderTools rebuilderTools) {
        super(rebuilderTools);
    }

    @Override
    public CompletionStage<FindCoordinatorRequest> rebuildRequest(FindCoordinatorRequest request, ClientRequest clientRequest) {
        return CompletableFuture.completedFuture(request);
    }

    @Override
    public CompletionStage<FindCoordinatorResponse> rebuildResponse(FindCoordinatorResponse response, ClientRequest clientRequest) {

        handleSingleCoordinatorResponse(response);

        response.data().coordinators().forEach(coordinator -> {
            try {
                var endpoint = rebuilderTools.brokerManager().getGatewayByReal(coordinator.host(), coordinator.port());
                // if we cannot map we should not expose the underlying cluster
                if (endpoint == null) {
                    coordinator.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
                    coordinator.setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());
                    coordinator.setHost("");
                    coordinator.setPort(0);
                } else {
                    coordinator.setHost(endpoint.getHost());
                    coordinator.setPort(endpoint.getPort());
                }
            } catch (Exception exception) {
                log.error("Error happen when rebuild coordinator", exception);
            }

        });
        return CompletableFuture.completedFuture(response);
    }

}

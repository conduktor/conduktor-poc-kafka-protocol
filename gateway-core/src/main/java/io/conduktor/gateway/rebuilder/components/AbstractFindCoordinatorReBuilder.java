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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;

import java.util.concurrent.CompletionStage;

@Slf4j
public abstract class AbstractFindCoordinatorReBuilder extends AbstractReBuilder<FindCoordinatorRequest, FindCoordinatorResponse> {

    public AbstractFindCoordinatorReBuilder(RebuilderTools rebuilderTools) {
        super(ApiKeys.FIND_COORDINATOR, rebuilderTools);
    }

    @Override
    public abstract CompletionStage<FindCoordinatorRequest> rebuildRequest(FindCoordinatorRequest request, ClientRequest clientRequest);

    @Override
    public abstract CompletionStage<FindCoordinatorResponse> rebuildResponse(FindCoordinatorResponse response, ClientRequest clientRequest);

    protected void handleSingleCoordinatorResponse(FindCoordinatorResponse response) {

        // for old requests we have to handle the single coordinator response
        var singleEndpoint = rebuilderTools.brokerManager().getGatewayByReal(response.data().host(), response.data().port());
        if (singleEndpoint == null) {
            // don't error here as newer versions will use the coordinator list instead
            response.data().setHost("");
            response.data().setPort(0);
        } else {
            response.data().setHost(singleEndpoint.getHost());
            response.data().setPort(singleEndpoint.getPort());
        }
    }

}

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
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Slf4j
public class DefaultMetadataReBuilder extends AbstractMetadataReBuilder {


    public DefaultMetadataReBuilder(RebuilderTools rebuilderTools) {
        super(rebuilderTools);
    }

    @Override
    public CompletionStage<MetadataRequest> rebuildRequest(MetadataRequest request, ClientRequest clientRequest) {
        return CompletableFuture.completedFuture(request);
    }

    @Override
    public CompletionStage<MetadataResponse> rebuildResponse(MetadataResponse response, ClientRequest clientRequest) {
        rebuildHost(response);
        return CompletableFuture.completedFuture(response);
    }


}

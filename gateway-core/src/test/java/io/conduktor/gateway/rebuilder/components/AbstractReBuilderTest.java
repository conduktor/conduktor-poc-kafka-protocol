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
import io.conduktor.gateway.service.InterceptorOrchestration;
import io.conduktor.gateway.service.RebuilderTools;
import io.conduktor.gateway.service.SerdeService;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractReBuilderTest {

    private RebuilderTools mockRebuilderTools;
    private SerdeService mockSerdeService;

    private InterceptorOrchestration mockInterceptorOrchestration;

    protected RequestHeader getRequestHeader() {
        var requestHeaderData = new RequestHeaderData();
        var requestHeader = new RequestHeader(requestHeaderData, ApiKeys.FETCH.latestVersion());
        return requestHeader;
    }

    @BeforeEach
    private void setup() {
        setupMocks();
    }

    private void setupMocks() {
        mockRebuilderTools = mock(RebuilderTools.class);
        mockInterceptorOrchestration = mock(InterceptorOrchestration.class);
        mockSerdeService = mock(SerdeService.class);

        when(mockRebuilderTools.interceptorOrchestration()).thenReturn(mockInterceptorOrchestration);
        when(mockRebuilderTools.serdeService()).thenReturn(mockSerdeService);
    }

    private class TestReBuilderWithError extends AbstractReBuilder<ProduceRequest, ProduceResponse> {

        public TestReBuilderWithError(ApiKeys apiKeys, RebuilderTools rebuilderTools) {
            super(apiKeys, rebuilderTools);
        }

        @Override
        public CompletionStage<ProduceResponse> rebuildResponse(ProduceResponse response, ClientRequest clientRequest) {
            return CompletableFuture.supplyAsync(() -> {
                throw new IllegalStateException();
            });
        }

    }

    private class TestReBuilderWithSuccess extends AbstractReBuilder<ProduceRequest, ProduceResponse> {

        public TestReBuilderWithSuccess(ApiKeys apiKeys, RebuilderTools rebuilderTools) {
            super(apiKeys, rebuilderTools);
        }

        @Override
        public CompletionStage<ProduceResponse> rebuildResponse(ProduceResponse response, ClientRequest clientRequest) {
            return CompletableFuture.completedFuture(response);
        }

    }

}


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

package io.conduktor.gateway.service;

import io.conduktor.gateway.interceptor.DirectionType;
import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.InterceptorContext;
import io.conduktor.gateway.interceptor.InterceptorValue;
import io.conduktor.gateway.model.InterceptContext;
import org.apache.kafka.common.requests.AbstractRequestResponse;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.stream.Collectors.toCollection;

public class InterceptorOrchestration {

    private final InterceptorPoolService interceptorPoolService;


    @Inject
    public InterceptorOrchestration(InterceptorPoolService interceptorPoolService) {
        this.interceptorPoolService = interceptorPoolService;
    }

    public CompletionStage<AbstractRequestResponse> intercept(InterceptContext interceptContext, AbstractRequestResponse input) {

        var interceptors = interceptorPoolService.getAllInterceptors(input.getClass()).stream()
                .sorted(Comparator.comparingInt(InterceptorValue::priority))
                .map(InterceptorValue::interceptor)
                .collect(toCollection(ConcurrentLinkedQueue::new));

        // we should only reset inflight info on request and on the first interceptor in the chain
        if (interceptContext.getDirectionType().equals(DirectionType.REQUEST)) {
            interceptContext.getClientRequest().setInflightInfo(new HashMap<String,Object>());
        }
        return intercept(interceptContext, interceptors, input);
    }

    private CompletionStage<AbstractRequestResponse> intercept(InterceptContext interceptContext,
                                                               ConcurrentLinkedQueue<Interceptor<AbstractRequestResponse>> interceptors,
                                                               AbstractRequestResponse input) {
        if (interceptors.isEmpty()) {
            return CompletableFuture.completedFuture(input);
        }

        return intercept(interceptContext, interceptors.poll(), input)
                .thenCompose(intercepted -> intercept(interceptContext, interceptors, intercepted));
    }


    @SuppressWarnings("unchecked")
    private CompletionStage<AbstractRequestResponse> intercept(InterceptContext interceptContext,
                                                               Interceptor<AbstractRequestResponse> interceptor,
                                                               AbstractRequestResponse input) {
        return interceptor.intercept(input, new InterceptorContext(
                interceptContext.getDirectionType(),
                interceptContext.getClientRequest().getGatewayRequestHeader(),
                (Map<String,Object>) interceptContext.getClientRequest().getInflightInfo(),
                interceptContext.getClientRequest().getClientChannel().remoteAddress()));
    }

}

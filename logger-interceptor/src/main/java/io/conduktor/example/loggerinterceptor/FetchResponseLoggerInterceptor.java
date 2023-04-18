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

package io.conduktor.example.loggerinterceptor;

import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.InterceptorContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.FetchResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Slf4j
public class FetchResponseLoggerInterceptor implements Interceptor<FetchResponse> {
    @Override
    public CompletionStage<FetchResponse> intercept(FetchResponse input, InterceptorContext interceptorContext) {
        log.warn("Fetch from client {} was responded to", interceptorContext.inFlightInfo().get("source"));
        return CompletableFuture.completedFuture(input);
    }
}
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
import org.apache.kafka.common.requests.AbstractRequestResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Slf4j
public class AllLoggerInterceptor implements Interceptor<AbstractRequestResponse> {

    private final String prefix;

    public AllLoggerInterceptor(String prefix) {
        this.prefix = prefix;
    }
    @Override
    public CompletionStage<AbstractRequestResponse> intercept(AbstractRequestResponse input, InterceptorContext interceptorContext) {
        log.warn("{}, a {} was sent/received", prefix, input.getClass());
        return CompletableFuture.completedFuture(input);
    }
}
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

package io.conduktor.gateway.interceptor;

import org.apache.kafka.common.requests.AbstractRequestResponse;

import java.lang.reflect.Modifier;
import java.util.*;

public interface Plugin {

    int EXPECTED_PARAMETER_COUNT = 2;
    String INTERCEPT_METHOD = "intercept";

    void configure(Map<String,Object> config) throws InterceptorConfigurationException;

    List<Interceptor> getInterceptors();

    default Map<Class<?>, List<Interceptor>> getTypedInterceptors() {
        var typedInterceptors = new HashMap<Class<?>, List<Interceptor>>();
        getInterceptors().forEach(gatewayInterceptor -> {
            var requestType = Arrays.stream(gatewayInterceptor.getClass().getDeclaredMethods())
                    .filter(method -> Modifier.isPublic(method.getModifiers()) &&
                            !method.isBridge() &&
                            method.getName().equals(INTERCEPT_METHOD) &&
                            method.getParameterCount() == EXPECTED_PARAMETER_COUNT &&
                            AbstractRequestResponse.class.isAssignableFrom(method.getParameterTypes()[0]) &&
                            method.getParameterTypes()[1].equals(InterceptorContext.class))
                    .findFirst().get()
                    .getParameterTypes()[0];
            if (!typedInterceptors.containsKey(requestType)) {
                typedInterceptors.put(requestType, new ArrayList<>());
            }
            typedInterceptors.get(requestType).add(gatewayInterceptor);
        });
        return typedInterceptors;
    }
}

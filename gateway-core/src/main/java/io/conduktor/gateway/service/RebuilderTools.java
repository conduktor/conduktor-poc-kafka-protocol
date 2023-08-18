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

import com.google.inject.Inject;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.network.BrokerManager;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class RebuilderTools {

    private final BrokerManager brokerManager;

    private final InterceptorOrchestration interceptorOrchestration;
    private final SerdeService serdeService;

    private final MetricsRegistryProvider metricsRegistryProvider;

    private final ClientService clientService;

    @Inject
    public RebuilderTools(BrokerManager brokerManager,
                          InterceptorOrchestration interceptorOrchestration,
                          MetricsRegistryProvider metricsRegistryProvider,
                          ClientService clientService) {

        this.brokerManager = brokerManager;
        this.interceptorOrchestration = interceptorOrchestration;
        this.serdeService = new SerdeService();
        this.metricsRegistryProvider = metricsRegistryProvider;
        this.clientService = clientService;
    }

}

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

package io.conduktor.gateway;

import com.google.inject.Inject;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.network.BrokerManager;
import io.conduktor.gateway.thread.UpStreamResource;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class GatewayExecutor implements AutoCloseable {

    private final UpStreamResource upStreamResource;
    private final BrokerManager brokerManager;
    private final GatewayConfiguration gatewayConfiguration;
    private final MetricsRegistryProvider metricsRegistryProvider;


    @Inject
    public GatewayExecutor(UpStreamResource upStreamResource,
                           GatewayConfiguration gatewayConfiguration,
                           BrokerManager brokerManager,
                           MetricsRegistryProvider metricsRegistryProvider) {
        this.upStreamResource = upStreamResource;
        this.brokerManager = brokerManager;
        this.gatewayConfiguration = gatewayConfiguration;
        this.metricsRegistryProvider = metricsRegistryProvider;
    }

    public void start() {
        try {
            brokerManager.setUpstreamResourceAndStartBroker(upStreamResource);
            log.info("Gateway started successfully with port range: {}", gatewayConfiguration.getHostPortConfiguration().getPortRange());
        } catch (Exception ex) {
            log.error("Error when starting Gateway", ex);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

    }

    @Override
    public void close() {
        log.error("Start to close resources!!!");
        try {
            if (upStreamResource != null) {
                upStreamResource.shutdownGracefully();
            }
            if (metricsRegistryProvider != null) {
                metricsRegistryProvider.close();
            }
            if (Objects.nonNull(brokerManager)) {
                brokerManager.close();
            }
        } catch (Exception e) {
            log.error("Error happen when close metric registry provider");
            throw new RuntimeException(e);
        }

    }

}

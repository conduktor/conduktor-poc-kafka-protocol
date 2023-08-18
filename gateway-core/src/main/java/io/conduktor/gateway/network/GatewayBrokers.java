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

package io.conduktor.gateway.network;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.conduktor.gateway.config.HostPortConfiguration;
import io.conduktor.gateway.metrics.MetricsRegistryKeys;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.micrometer.core.instrument.Gauge;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.conduktor.gateway.config.support.Messages.ERROR_ACTIVATING_GATEWAY_PORT;

@Slf4j
public class GatewayBrokers {

    @Getter
    private final EventLoopGroup bossGroup;
    private final Map<Integer, Channel> gatewayBrokers;
    private final Map<Integer, GatewayChannelInitializer> gatewayChannelInitializers;
    private final Gauge gauge;
    private final String gatewayBindHost;

    @Inject
    public GatewayBrokers(@Named("downstreamThread") Integer downstreamThread,
                          HostPortConfiguration hostPortConfiguration,
                          MetricsRegistryProvider metricsRegistryProvider) {
        this.bossGroup = new NioEventLoopGroup(downstreamThread);
        this.gatewayBrokers = new HashMap<>();
        this.gatewayChannelInitializers = new HashMap<>();
        this.gatewayBindHost = hostPortConfiguration.getGatewayBindHost();
        this.gauge = Gauge
                .builder(MetricsRegistryKeys.BROKERED_ACTIVE_CONNECTIONS, this.gatewayBrokers::size)
                .description("Gathers active /to/ broker connections of the connection pool")
                .register(metricsRegistryProvider.registry());
    }

    public void activateBroker(int port) {
        try {
            var gatewayChannelInitializer = gatewayChannelInitializers.get(port);
            gatewayChannelInitializer.activate();
            gauge.measure();
        } catch (Exception e) {
            log.error("Error when activating port of Gateway: [port={}],. Need to restart the Gateway! ", port, e);
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException ex) {
            }
            System.exit(ERROR_ACTIVATING_GATEWAY_PORT);
        }
    }

    public void acquirePort(int port, GatewayChannelInitializer channelInitializer) {
        try {
            gatewayChannelInitializers.put(port, channelInitializer);
            var channel = new ServerBootstrap()
                    .group(bossGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.DEBUG))
                    .childHandler(channelInitializer)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .bind(gatewayBindHost, port)
                    .sync()
                    .channel();
            gatewayBrokers.put(port, channel);
            gauge.measure();
            log.info("Gateway channel bound: {}", channel.localAddress());
        } catch (Exception e) {
            log.error("Error when init new port of Gateway: [port={}]", port, e);
            throw new RuntimeException(e);
        }
    }

    public void deactivateBrokersNotUsed(List<Integer> usedPort) {
        gatewayChannelInitializers.keySet().forEach(port -> {
            if (usedPort.contains(port)) {
                return;
            }
            deactivateBroker(port);
        });
    }

    public void close() {
        bossGroup.shutdownGracefully();
        gatewayBrokers.forEach((endpoint, ch) -> {
            final InetSocketAddress address = (InetSocketAddress) ch.localAddress();
            try {
                ch.close().await(5, TimeUnit.SECONDS);
                log.debug("Gateway server channel closed: {}:{}", address.getHostString(), address.getPort());
            } catch (final InterruptedException e) {
                log.error("Failed to close Gateway server channel: {}:{}", address.getHostString(), address.getPort(), e);
            }
        });
    }


    public void deactivateBroker(int gatewayPort) {
        var gatewayChannelInitializer = gatewayChannelInitializers.get(gatewayPort);
        if (Objects.isNull(gatewayChannelInitializer)) {
            return;
        }
        gatewayChannelInitializer.deactivate();
        gauge.measure();
    }

}


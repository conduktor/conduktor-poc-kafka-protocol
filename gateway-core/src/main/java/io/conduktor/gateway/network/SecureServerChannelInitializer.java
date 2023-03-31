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

import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.network.handler.CountingDuplexHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.DomainWildcardMappingBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.conduktor.gateway.common.Constants.SIZE_BYTES;


@Slf4j
public class SecureServerChannelInitializer extends GatewayChannelInitializer {

    private final MetricsRegistryProvider metricsRegistryProvider;
    private final Supplier<Map<String, SslContext>> sslContextSupplier;
    private final String gatewayHost;


    public SecureServerChannelInitializer(
            Supplier<Map<String, SslContext>> sslContextSupplier,
            String gatewayHost,
            MetricsRegistryProvider metricsRegistryProvider,
            Consumer<SocketChannel> logicHandler) {
        super(logicHandler);
        this.metricsRegistryProvider = metricsRegistryProvider;
        this.sslContextSupplier = sslContextSupplier;
        this.gatewayHost = gatewayHost;
    }


    @Override
    public void customMiddleHandlers(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        log.debug("Incoming connection on {}:{} from {}:{}",
                ch.localAddress().getHostString(),
                ch.localAddress().getPort(),
                ch.remoteAddress().getHostString(),
                ch.remoteAddress().getPort());
        var contexts = sslContextSupplier.get();
        var builder = new DomainWildcardMappingBuilder<SslContext>(1, contexts.values().iterator().next());
        contexts.forEach(builder::add);
        pipeline.addLast(new SniHandler(builder.build()));
        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, SIZE_BYTES, 0, SIZE_BYTES));
        ch.pipeline().addLast(new CountingDuplexHandler(metricsRegistryProvider));
    }

}

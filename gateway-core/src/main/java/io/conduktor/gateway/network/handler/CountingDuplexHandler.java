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

package io.conduktor.gateway.network.handler;

import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.micrometer.core.instrument.Counter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class CountingDuplexHandler extends ChannelDuplexHandler {

    private final Counter upstreamCounter;
    private final Counter downstreamCounter;

    public CountingDuplexHandler(MetricsRegistryProvider metricsRegistryProvider) {
        this.upstreamCounter = metricsRegistryProvider.getUpstreamCounter();
        this.downstreamCounter = metricsRegistryProvider.getDownstreamCounter();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            var len = ((ByteBuf) msg).readableBytes();
            upstreamCounter.increment(len);
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf) {
            var len = ((ByteBuf) msg).readableBytes();
            downstreamCounter.increment(len);
        }
        super.write(ctx, msg, promise);
    }

}

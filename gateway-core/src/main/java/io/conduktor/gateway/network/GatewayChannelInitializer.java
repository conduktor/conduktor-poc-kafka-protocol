

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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;


@Slf4j
public abstract class GatewayChannelInitializer extends ChannelInitializer<SocketChannel> {

    protected Consumer<SocketChannel> logicHandler;
    private volatile boolean isActive;
    private final ConcurrentLinkedQueue<Channel> activeChannel = new ConcurrentLinkedQueue<>();


    public GatewayChannelInitializer(Consumer<SocketChannel> logicHandler) {
        this.logicHandler = logicHandler;
    }

    public void activate() {
        closeCurrentConnections();
        this.isActive = true;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        if (!isActive) {
            log.debug("connection to inactive broker, close it. {}", ch.localAddress().getPort());
            ch.close();
        }
        addNewChannel(ch);
        customMiddleHandlers(ch);
        logicHandler.accept(ch);
    }

    public void deactivate() {
        closeCurrentConnections();
        this.isActive = false;
    }

    abstract void customMiddleHandlers(SocketChannel ch);

    protected void addNewChannel(Channel channel) {
        activeChannel.add(channel);
        channel.closeFuture().addListener(r -> {
            activeChannel.remove(channel);
        });
    }

    private void closeCurrentConnections() {
        activeChannel.forEach(ChannelOutboundInvoker::close);
        activeChannel.clear();
    }

}

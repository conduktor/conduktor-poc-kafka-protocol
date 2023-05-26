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

import io.conduktor.gateway.authorization.SecurityHandler;
import io.conduktor.gateway.model.User;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.thread.GatewayThread;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.util.AttributeKey;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.DelayedResponseAuthenticationException;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * connection from client to gateway
 */
@Slf4j
public class GatewayChannel extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private final SocketChannel gatewaySocketChannel;

    private final String gatewayHost;
    @Getter
    private final GatewayThread gatewayThread;
    @Getter
    private final SecurityHandler authenticator;
    private final BrokerManager brokerManager;
    private final CompletionStage<Void> closeFuture = new CompletableFuture<>();
    private final ConcurrentLinkedQueue<ClientRequest> pendingResponseRequests = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ClientRequest> pendingSendRequests = new ConcurrentLinkedQueue<>();
    private String hostName;

    @Setter
    private Node node;
    private ChannelState state;
    private InetAddress remoteAddress;

    private volatile boolean isClosed;
    private int successfulAuthentications;


    public GatewayChannel(SecurityHandler authenticator,
                          BrokerManager brokerManager,
                          SocketChannel gatewaySocketChannel,
                          GatewayThread gatewayThread,
                          String gatewayHost) {
        this.authenticator = authenticator;
        this.brokerManager = brokerManager;
        this.gatewayHost = gatewayHost;
        this.gatewayThread = gatewayThread;
        this.state = ChannelState.NOT_CONNECTED;
        this.gatewaySocketChannel = gatewaySocketChannel;
        if (gatewaySocketChannel != null) {
            this.remoteAddress = gatewaySocketChannel.remoteAddress().getAddress();
            gatewaySocketChannel.closeFuture().addListener(rs -> {
                if (rs.isSuccess()) {
                    this.close();
                }
            });
        }
    }


    @Override
    public void close() throws IOException {
        //always call this function to ensure we send response failure authentication to client
        authenticator.handleAuthenticationFailure()
                .whenComplete((rs, ex) -> {
                    gatewaySocketChannel.close();
                    try {
                        Utils.closeAll(authenticator);
                    } catch (IOException e) {
                        log.error("Error happen when close authenticator. ", e);
                    }
                    closeFuture.toCompletableFuture().complete(null);
                    pendingSendRequests.clear();
                    pendingResponseRequests.clear();
                    this.isClosed = true;
                });
    }

    public boolean isClosed() {
        return isClosed;
    }

    public CompletionStage<Void> closeFuture() {
        return this.closeFuture;
    }


    public boolean ready() {
        return authenticator.complete();
    }

    @Override
    public int hashCode() {
        return gatewaySocketChannel.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GatewayChannel that = (GatewayChannel) o;
        return gatewaySocketChannel.equals(that.gatewaySocketChannel);
    }

    @Override
    public String toString() {
        return super.toString() + " gatewayChannel=" + gatewaySocketChannel;
    }

    public Node getNode() {
        return node;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        activateChannel();
    }

    public String socketDescription() {
        if (gatewaySocketChannel.localAddress().getAddress() == null)
            return gatewaySocketChannel.localAddress().toString();
        return gatewaySocketChannel.localAddress().getAddress().toString();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        var buf = (ByteBuf) msg;
        try {
            handleIncomingRequest(buf);
        } catch (Exception e) {
            String desc = String.format("%s (channelId=%s)", socketDescription(), gatewaySocketChannel.id());
            if (e instanceof IOException) {
                log.debug("Connection with {} disconnected", desc, e);
            } else if (e instanceof AuthenticationException) {
                String exceptionMessage = e.getMessage();
                if (e instanceof DelayedResponseAuthenticationException)
                    exceptionMessage = e.getCause().getMessage();
                log.info("Failed authentication with {} ({})", desc, exceptionMessage);
            } else {
                log.warn("Unexpected error from {}; closing connection", desc, e);
            }
            close();
        } finally { //avoid buffer leaks
            buf.release();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof SniCompletionEvent sniEvent) {
            ctx.channel().attr(AttributeKey.valueOf("hostName")).set(sniEvent.hostname());
            activateChannel();
        }
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator.
     * For SSL with client authentication enabled, {@link TransportLayer#handshake()} performs
     * authentication. For SASL, authentication is performed by {@link Authenticator#authenticate()}.
     */
    public void handleIncomingRequest(ByteBuf byteBuf) throws Exception {
        boolean authenticating = false;
        try {
            if (!authenticator.complete()) {
                authenticating = true;
                authenticator.authenticate(byteBuf);
                return;
            }
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            if (authenticating) {
                throw new DelayedResponseAuthenticationException(e);
            }
            throw e;
        }
        if (ready()) {
            ++successfulAuthentications;
            state = ChannelState.READY;
        }
        gatewayThread.justSend(byteBuf, this);
    }

    public void sendResponse() {
        if (pendingResponseRequests.peek() != null && pendingResponseRequests.peek().isDone()) {
            var doneRequest = pendingResponseRequests.poll();
            gatewaySocketChannel.writeAndFlush(doneRequest.getResponse());
            sendResponse();
        }
    }


    public ClientRequest nextRequestToSend() {
        var next = pendingSendRequests.peek();
        if (Objects.isNull(next)) {
            return null;
        }
        //to handle requests that are already complete on the Gateway, no need to send to server
        if (next.isDone()) {
            pendingSendRequests.poll();
            return nextRequestToSend();
        }
        if (next.isReadyToSend()) {
            return next;
        }
        return null;
    }

    public void pollTopToSend() {
        pendingSendRequests.poll();
    }


    public void enqueueRequest(ClientRequest request) {
        pendingResponseRequests.add(request);
        pendingSendRequests.add(request);
    }

    public SocketChannel getGatewaySocketChannel() {
        return gatewaySocketChannel;
    }

    public User getUser() {
        return authenticator.getUser()
                .orElse(User.ANONYMOUS);
    }


    private void activateChannel() {
        this.hostName = (String) gatewaySocketChannel.attr(AttributeKey.valueOf("hostName")).get();
        this.node = brokerManager.getRealNodeByGateway(gatewaySocketChannel);
    }

}

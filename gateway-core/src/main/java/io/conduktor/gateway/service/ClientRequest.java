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

import com.google.common.annotations.VisibleForTesting;
import io.conduktor.gateway.common.KafkaRequestUtils;
import io.conduktor.gateway.model.User;
import io.conduktor.gateway.network.GatewayChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * it like a temporary cache for one request from client.
 * it's lifecycle will end when we return response to client
 * we should put all the information related to client request to this.
 */
@Getter
public class ClientRequest {

    private final SocketChannel clientChannel;
    private final String connectionId;
    private final RequestHeader clientRequestHeader;
    private final ByteBuffer request;
    private final long initiatedNanos;
    private final GatewayChannel gatewayChannel;
    private final Integer nodeId;
    /**
     * gateway request header, for now, it just has different correlation id, compare to {@link #clientRequestHeader}
     */
    private RequestHeader gatewayRequestHeader;
    private ByteBuf response;
    private NetworkSend toSendKafka;

    private Consumer<ClientRequest> schedulerToSendToKafka;
    private Consumer<ClientRequest> schedulerToSendResponse;
    private volatile boolean isDone;
    private volatile boolean isReadyToSend;
    private volatile boolean isReadyToReceiver;

    private volatile boolean recordingMetrics = true;

    private long sendToKafkaStartTime;

    @Setter
    @Getter
    private Object inflightInfo;

    @Setter
    @Getter
    private Object inflightCacheInfo;


    @VisibleForTesting
    public ClientRequest(RequestHeader clientRequestHeader, ByteBuffer request, String connectionId) {
        this.clientChannel = null;
        this.gatewayChannel = null;
        this.clientRequestHeader = clientRequestHeader;
        this.connectionId = connectionId;
        this.request = request;
        this.initiatedNanos = Time.SYSTEM.nanoseconds();
        this.nodeId = null;
    }

    @VisibleForTesting
    public ClientRequest(RequestHeader clientRequestHeader, ByteBuffer request, String connectionId, int nodeId) {
        this.clientChannel = null;
        this.gatewayChannel = null;
        this.clientRequestHeader = clientRequestHeader;
        this.connectionId = connectionId;
        this.request = request;
        this.initiatedNanos = Time.SYSTEM.nanoseconds();
        this.nodeId = nodeId;
    }

    private ClientRequest(GatewayChannel gatewayChannel,
                          RequestHeader clientRequestHeader,
                          String connectionId,
                          ByteBuffer request,
                          Integer nodeId,
                          Consumer<ClientRequest> schedulerToSendResponse,
                          Consumer<ClientRequest> schedulerToSendToKafka) {
        this.clientChannel = gatewayChannel.getGatewaySocketChannel();
        this.gatewayChannel = gatewayChannel;
        this.clientRequestHeader = clientRequestHeader;
        this.connectionId = connectionId;
        this.request = request;
        this.initiatedNanos = Time.SYSTEM.nanoseconds();
        this.nodeId = nodeId;
        this.schedulerToSendToKafka = schedulerToSendToKafka;
        this.schedulerToSendResponse = schedulerToSendResponse;
    }

    public static ClientRequest initRequest(GatewayChannel gatewayChannel,
                                            RequestHeader clientRequestHeader,
                                            ByteBuffer request,
                                            String connectionId,
                                            InFlightRequestService inFlightRequestService,
                                            Consumer<ClientRequest> schedulerToSendResponse,
                                            Consumer<ClientRequest> schedulerToSendToKafka) {
        var clientRequest = new ClientRequest(gatewayChannel,
                clientRequestHeader,
                connectionId,
                request.duplicate(),
                gatewayChannel.getNode().id(),
                schedulerToSendResponse,
                schedulerToSendToKafka);
        var gatewayCorrelationId = inFlightRequestService.trackRequest(clientRequest);
        clientRequest.initGatewayRequestHeader(gatewayCorrelationId);
        return clientRequest;
    }

    public <T> T getInflightInfo(Class<T> tClass) {
        return ((T) inflightInfo);
    }

    public void initGatewayRequestHeader(int gatewayCorrelationId) {
        var newRequestHeader = KafkaRequestUtils.duplicateRequestHeader(clientRequestHeader);
        newRequestHeader.data().setCorrelationId(gatewayCorrelationId);
        this.gatewayRequestHeader = newRequestHeader;
    }

    public NetworkSend getToSendKafka() {
        return toSendKafka;
    }

    public void readyToSendToKafka(NetworkSend toSendKafka) {
        this.toSendKafka = toSendKafka;
        this.isReadyToSend = true;
        schedulerToSendToKafka.accept(this);
        this.sendToKafkaStartTime = Time.SYSTEM.milliseconds();
    }

    public void readyToSendBackClient() {
        this.isReadyToReceiver = true;
    }

    public void marKDoneWithResponse(ByteBuf response) {
        this.response = response;
        this.isDone = true;
        schedulerToSendResponse.accept(this);
    }

    @Override
    public String toString() {
        return "ClientRequest{" +
                "gatewayCorrelationId=" + getGatewayCorrelationId() +
                ", clientChannel=" + clientChannel +
                ", requestHeader=" + clientRequestHeader +
                ", nodeId='" + connectionId + '\'' +
                '}';
    }

    public int getClientCorrelationId() {
        return clientRequestHeader.data().correlationId();
    }

    public int getGatewayCorrelationId() {
        return gatewayRequestHeader.data().correlationId();
    }

    public void setRecordingMetrics(boolean recordingMetrics) {
        this.recordingMetrics = recordingMetrics;
    }

    public boolean isRecordingMetrics() {
        return this.recordingMetrics;
    }

    public User getUser() {
        return gatewayChannel.getUser();
    }
}

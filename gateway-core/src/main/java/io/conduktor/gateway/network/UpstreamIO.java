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

import io.conduktor.gateway.config.ConnectionConfig;
import io.conduktor.gateway.metrics.MetricsRegistryKeys;
import io.conduktor.gateway.service.ClientRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.conduktor.gateway.common.NodeUtils.keyOf;

/**
 * a wrapper class, use {@link Selector} under the hood for IO operation with kafka cluster.
 */
@Slf4j
public class UpstreamIO implements Closeable {

    private final int numOfConnection;
    private final long connectionMaxIdleMS;

    private final AtomicInteger lastConnectionId = new AtomicInteger(0);
    private final Selector selector;
    /**
     * use for round-robin
     */
    private final ConcurrentHashMap<String, AtomicInteger> nodeWithConnectionAssignCounter = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, UpStreamConnection[]> nodeWithConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, UpStreamConnection> upstreamConnectionMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GatewayChannel, UpStreamConnection> downstreamChannel = new ConcurrentHashMap<>();
    private final int maxSendSize;
    private final int maxReceiveSize;
    private final AbstractConfig selectorConfig;


    public UpstreamIO(Properties selectorProps,
                      ConnectionConfig connectionConfig) {
        //Just to satisfied producer config, it means nothing.
        selectorProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        selectorProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        selectorConfig = new ProducerConfig(Utils.propsToMap(selectorProps));
        maxSendSize = selectorConfig.getInt(ProducerConfig.SEND_BUFFER_CONFIG);
        maxReceiveSize = selectorConfig.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG);
        this.numOfConnection = connectionConfig.getNumOfConnection();
        this.connectionMaxIdleMS = connectionConfig.getMaxIdleTimeMs();
        this.selector = newKafkaSelector();
    }


    public void poll(long timeout) throws IOException {
        selector.poll(timeout);
    }

    public Collection<NetworkReceive> completedReceives() {
        return selector.completedReceives();
    }

    public void wakeup() {
        selector.wakeup();
    }

    public Collection<String> disconnected() {
        var disconnected = this.selector.disconnected();
        if (disconnected.size() > 0) {
            log.debug("disconnected");
        }
        return this.selector.disconnected().keySet();
    }

    public void send(NetworkSend networkSend) {
        selector.send(networkSend);
    }

    /**
     * just use round-robin for now if need to assign
     *
     * @param gatewayChannel == client <-> gateway connection
     * @return a connection to kafka server
     */
    public UpStreamConnection getAssociatedConnection(GatewayChannel gatewayChannel) {
        return downstreamChannel.computeIfAbsent(gatewayChannel, c -> {
            var node = gatewayChannel.getNode();
            var counter = nodeWithConnectionAssignCounter.get(keyOf(node));
            var pool = nodeWithConnections.get(keyOf(node));
            var connection = pool[counter.getAndIncrement() & pool.length - 1];
            connection.trackDownStreamConnection(gatewayChannel);
            gatewayChannel.closeFuture().thenAccept(rs -> downstreamChannel.remove(gatewayChannel));
            connectIfNeeded(connection);
            return connection;
        });
    }

    public void registerMetrics(MeterRegistry meterRegistry, Tags threadTags) {
        meterRegistry.gauge(MetricsRegistryKeys.UPSTREAMIO_CONNECTIONS_UPSTREAM_CONNECTED, threadTags, selector.connected().size());
        meterRegistry.gaugeMapSize(MetricsRegistryKeys.UPSTREAMIO_CONNECTIONS_DOWNSTREAM, threadTags, downstreamChannel);
        meterRegistry.gaugeMapSize(MetricsRegistryKeys.UPSTREAMIO_NODES, threadTags, nodeWithConnections);
    }


    public void registerNode(Node node) {
        nodeWithConnections.computeIfAbsent(keyOf(node), (k) -> {
            var connectionQueue = new UpStreamConnection[numOfConnection];
            for (int i = 0; i < numOfConnection; i++) {
                var connectionId = String.valueOf(lastConnectionId.getAndIncrement());
                var connection = new UpStreamConnection(connectionId, node);
                connectionQueue[i] = connection;
                upstreamConnectionMap.put(connectionId, connection);
            }
            nodeWithConnectionAssignCounter.put(keyOf(node), new AtomicInteger(0));
            return connectionQueue;
        });
    }

    public void deregisterKafkaNode(Node node) {
        var connections = nodeWithConnections.remove(keyOf(node));
        nodeWithConnectionAssignCounter.remove(keyOf(node));
        for (var connection : connections) {
            connection.disconnect();
            upstreamConnectionMap.remove(connection.getConnectionId());
        }
    }

    public void disconnect(String connectionId) {
        if (upstreamConnectionMap.containsKey(connectionId)) {
            upstreamConnectionMap.get(connectionId).disconnect();
        }
    }

    @Override
    public void close() throws IOException {
        nodeWithConnections.forEach((keyOfNode, connections) -> {
            for (var connection : connections) {
                selector.close(connection.getConnectionId());
                connection.disconnect();
            }
        });
    }


    public boolean trySend(ClientRequest request) {
        var ableToSend = new AtomicBoolean();
        downstreamChannel.computeIfPresent(request.getGatewayChannel(), (unused, upStreamConnection) -> {
            if (isNodeAbleToSend(upStreamConnection.getConnectionId())) {
                selector.send(request.getToSendKafka());
                ableToSend.set(true);
                return upStreamConnection;
            }
            ableToSend.set(false);
            return upStreamConnection;
        });
        return ableToSend.get();
    }


    Selector newKafkaSelector() {
        var time = Time.SYSTEM;
        LogContext logContext = new LogContext();
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(selectorConfig, time, logContext);
        return new Selector(connectionMaxIdleMS,
                new Metrics(), time, "producer", channelBuilder, logContext);
    }

    private boolean isNodeAbleToSend(String connectionId) {
        return selector.isChannelReady(connectionId) && selector.channel(connectionId) != null && !selector.channel(connectionId).hasSend();
    }

    private void connectIfNeeded(UpStreamConnection upStreamConnection) {
        if (selector.isChannelReady(upStreamConnection.getConnectionId())) {
            return;
        }
        try {
            connect(upStreamConnection);
        } catch (IOException e) {
            log.warn("error happen when try to connect to kafka server: ", e);
            upStreamConnection.disconnect();
        }
    }

    private void connect(UpStreamConnection upStreamConnection) throws IOException {
        var node = upStreamConnection.getNode();
        var host = node.host();
        var port = node.port();
        var connectionId = upStreamConnection.getConnectionId();
        try {
            log.debug("Initiating connection to host {} and port {} with id {}", host, port, connectionId);
            selector.connect(connectionId,
                    new InetSocketAddress(host, port),
                    maxSendSize,
                    maxReceiveSize);
        } catch (IllegalStateException illegalStateException) {
            log.warn("connection id {} already connect", connectionId, illegalStateException);
        }
    }

}

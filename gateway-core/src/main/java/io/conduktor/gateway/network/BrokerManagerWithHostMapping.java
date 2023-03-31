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

import com.google.inject.name.Named;
import io.conduktor.gateway.config.AuthenticationConfig;
import io.conduktor.gateway.config.Endpoint;
import io.conduktor.gateway.config.HostPortConfiguration;
import io.conduktor.gateway.config.GatewayPortAndKafkaNodePair;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.service.ClientService;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Node;

import javax.inject.Inject;
import java.util.*;

import static io.conduktor.gateway.common.NodeUtils.keyOf;

@Slf4j
public class BrokerManagerWithHostMapping extends BrokerManager {

    private final Integer gatewayPort;
    private final String hostPrefix;
    private Map<String, PrefixWithPort> realHostToGateway = new HashMap<>();
    private Map<String, Node> gatewayPrefixToRealNode = new HashMap<>();
    private List<Node> nodes = new ArrayList<>();

    @Inject
    public BrokerManagerWithHostMapping(@Named("kafkaServerProperties") Properties properties,
                                        GatewayBrokers gatewayBrokers,
                                        AuthenticationConfig authenticationConfig,
                                        HostPortConfiguration hostPortConfiguration,
                                        MetricsRegistryProvider metricsRegistryProvider,
                                        ClientService clientService) {
        super(properties, authenticationConfig, hostPortConfiguration, metricsRegistryProvider, gatewayBrokers,
                clientService);
        this.gatewayPort = hostPortConfiguration.getGatewayPort();
        this.hostPrefix = hostPortConfiguration.getHostPrefix();
    }

    @Override
    public void close() throws Exception {
        if (gatewayBrokers != null) {
            gatewayBrokers.close();
        }
    }

    @Override
    public synchronized Map<String, Endpoint> getRealToGatewayMap(List<Node> brokers) {
        var newRealHostToGatewayEndpoint = new HashMap<String, PrefixWithPort>();
        var newGatewayPrefixToNodeMap = new HashMap<String, Node>();
        var newNodes = new ArrayList<Node>();
        brokers.forEach(broker -> {
            var nodeId = broker.id();
            var gatewayPrefix = hostPrefix + nodeId;
            var gateway = new PrefixWithPort(gatewayPrefix, gatewayPort);
            newNodes.add(broker);
            newRealHostToGatewayEndpoint.put(keyOf(broker.host(), broker.port()), gateway);
            newGatewayPrefixToNodeMap.put(gatewayPrefix, broker);
        });
        //register new nodes
        newNodes.stream().filter(newNode -> !nodes.contains(newNode))
                .forEach(upStreamResource::registerKafkaNode);
        //deregister old node
        nodes.stream().filter(newNode -> !newNodes.contains(newNode))
                .forEach(upStreamResource::deregisterKafkaNode);
        nodes = newNodes;
        gatewayPrefixToRealNode = newGatewayPrefixToNodeMap;
        realHostToGateway = newRealHostToGatewayEndpoint;
        return buildHostMap();
    }

    @Override
    public Endpoint getGatewayByReal(String host, int port) {
        var key = keyOf(host, port);
        if (!realHostToGateway.containsKey(key)) {
            return null;
        }
        var prefix = realHostToGateway.get(key);
        return buildEndpoint(prefix);
    }

    @Override
    public Node getRealNodeByGateway(SocketChannel socketChannel) {
        var hostName = (String) socketChannel.attr(AttributeKey.valueOf("hostName")).get();
        if (StringUtils.isBlank(hostName) || gatewayPrefixToRealNode.isEmpty()) {
            return firstNode;
        }
        int idx = hostName.indexOf('.');
        if (idx != -1) {
            var brokerPrefix = hostName.substring(0, idx);
            var node = gatewayPrefixToRealNode.get(brokerPrefix);
            if (Objects.nonNull(node)) {
                return node;
            }

        }
        int index = (int) (Math.random() * nodes.size());
        return nodes.get(index);
    }
    @Override
    protected void startBroker() {
        var newBrokerMapping = new GatewayPortAndKafkaNodePair(gatewayPort, firstNode);
        upStreamResource.registerKafkaNode(firstNode);
        gatewayBrokers.acquirePort(gatewayPort, channelInitializerSupplier.get());
        initGatewayBroker(newBrokerMapping);
    }

    private Map<String, Endpoint> buildHostMap() {
        var hostMap = new HashMap<String, Endpoint>();
        realHostToGateway.forEach((realHost, prefixAndPort) -> hostMap.put(realHost, buildEndpoint(prefixAndPort)));
        return hostMap;
    }

    private Endpoint buildEndpoint(PrefixWithPort prefixWithPort) {
        var host = prefixWithPort.prefix + "." + gatewayHost;
        return new Endpoint(host, prefixWithPort.port);
    }

    private void initGatewayBroker(GatewayPortAndKafkaNodePair gatewayPortAndKafkaNodePair) {
        var node = gatewayPortAndKafkaNodePair.getRealClusterNode();
        log.info("Initializing gateway {} for node {}", gatewayPortAndKafkaNodePair.getGatewayPort(), node);
        var gatewayPort = gatewayPortAndKafkaNodePair.getGatewayPort();
        gatewayBrokers.activateBroker(gatewayPort);
    }


    record PrefixWithPort(String prefix, int port) {

    }


}

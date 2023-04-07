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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.Node;

import javax.inject.Inject;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static io.conduktor.gateway.common.NodeUtils.keyOf;

@Slf4j
public class BrokerManagerWithPortMapping extends BrokerManager {

    public static int PORT_NOT_ENOUGH_EXIT_CODE = 70;

    private final List<Integer> gatewayPorts;

    private Map<String, GatewayPortAndKafkaNodePair> realHostToPortMappings = new HashMap<>();
    private Map<Integer, Node> portToNodeMap = new HashMap<>();

    @Inject
    public BrokerManagerWithPortMapping(@Named("kafkaNodes") List<Node> nodes,
                                        GatewayBrokers gatewayBrokers,
                                        HostPortConfiguration hostPortConfiguration,
                                        AuthenticationConfig authenticationConfig,
                                        MetricsRegistryProvider metricsRegistryProvider) {
        super(nodes, authenticationConfig, hostPortConfiguration, metricsRegistryProvider, gatewayBrokers);
        this.gatewayPorts = hostPortConfiguration.getPortInRange();
        validatePortRangeWithKafkaNodes(nodes);
    }

    private void validatePortRangeWithKafkaNodes(@NonNull List<Node> nodes) {
        if (gatewayPorts.size() < nodes.size()) {
            var portSize = CollectionUtils.isEmpty(gatewayPorts) ? 0 : gatewayPorts.size();
            var numOfBroker = CollectionUtils.isEmpty(nodes) ? 0 : nodes.size();
            log.error("Port mapping: port range has {} ports, which is less than {} - number of kafka brokers.", portSize, numOfBroker);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            System.exit(PORT_NOT_ENOUGH_EXIT_CODE);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (gatewayBrokers != null) {
            gatewayBrokers.close();
        }
    }

    @Override
    public synchronized Map<String, Endpoint> getRealToGatewayMap(List<Node> brokers) {
        validatePortRangeWithKafkaNodes(brokers);
        var sortedBrokers = brokers.stream().sorted((b1, b2) -> {
            var compareHost = b1.host().compareTo(b2.host());
            if (compareHost != 0) {
                return compareHost;
            }
            if (b1.port() == b2.port()) {
                return 0;
            }
            return b1.port() < b2.port() ? -1 : 1;
        }).toList();
        var newMappings = new HashMap<String, GatewayPortAndKafkaNodePair>();
        for (int i = 0; i < sortedBrokers.size(); i++) {
            var broker = sortedBrokers.get(i);
            var node = new Node(broker.id(), broker.host(), broker.port());
            var gatewayPort = gatewayPorts.get(i);
            var pair = new GatewayPortAndKafkaNodePair(gatewayPort, node);
            newMappings.put(keyOf(broker.host(), broker.port()), pair);
        }
        var resetPorts = new ArrayList<Integer>();
        newMappings.forEach((kafkaBrokerKey, newPortAndNode) -> {
            var currentPortAndNode = realHostToPortMappings.get(kafkaBrokerKey);
            if (Objects.isNull(currentPortAndNode)) {
                //it is new kafka node
                upStreamResource.registerKafkaNode(newPortAndNode.getRealClusterNode());
                resetPorts.add(newPortAndNode.getGatewayPort());
                return;
            }
            if (!Objects.equals(currentPortAndNode, newPortAndNode)) {
                //mapping is changed, we should deactivate current broker and activate it with new node
                resetPorts.add(newPortAndNode.getGatewayPort());
            }
        });
        //clean old resources
        realHostToPortMappings.forEach((key, currentBroker) -> {
            if (!newMappings.containsKey(key)) {
                upStreamResource.deregisterKafkaNode(currentBroker.getRealClusterNode());
            }
        });
        var newActivePorts = newMappings.values().stream().map(GatewayPortAndKafkaNodePair::getGatewayPort).toList();
        gatewayBrokers.deactivateBrokersNotUsed(newActivePorts);
        setMap(newMappings);
        resetPorts.forEach(gatewayBrokers::activateBroker);
        return newMappings.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> new Endpoint(gatewayHost, entry.getValue().getGatewayPort())));
    }

    @Override
    public Endpoint getGatewayByReal(String host, int port) {
        var key = keyOf(host, port);
        if (!realHostToPortMappings.containsKey(key)) {
            return null;
        }
        var portAndNode = realHostToPortMappings.get(key);
        return new Endpoint(gatewayHost, portAndNode.getGatewayPort());
    }

    @Override
    public Node getRealNodeByGateway(SocketChannel socketChannel) {
        return portToNodeMap.getOrDefault(socketChannel.localAddress().getPort(), firstNode);
    }

    @Override
    public void startBroker() {
        var newBrokerMapping = new GatewayPortAndKafkaNodePair(gatewayPorts.get(0), firstNode);
        acquirePorts(gatewayPorts);
        this.upStreamResource.registerKafkaNode(firstNode);
        initGatewayBroker(newBrokerMapping);
        realHostToPortMappings.put(keyOf(firstNode), newBrokerMapping);
    }

    private void setMap(Map<String, GatewayPortAndKafkaNodePair> map) {
        this.realHostToPortMappings = map;
        this.portToNodeMap = map.values().stream().collect(Collectors.toMap(GatewayPortAndKafkaNodePair::getGatewayPort, GatewayPortAndKafkaNodePair::getRealClusterNode));
    }

    private void initGatewayBroker(GatewayPortAndKafkaNodePair gatewayPortAndKafkaNodePair) {
        var node = gatewayPortAndKafkaNodePair.getRealClusterNode();
        log.info("Initializing Gateway {} for node {}", gatewayPortAndKafkaNodePair.getGatewayPort(), node);
        var gatewayPort = gatewayPortAndKafkaNodePair.getGatewayPort();
        gatewayBrokers.activateBroker(gatewayPort);
    }

    private void acquirePorts(List<Integer> gatewayPorts) {
        for (var port : gatewayPorts) {
            gatewayBrokers.acquirePort(port, channelInitializerSupplier.get());
        }
    }


}

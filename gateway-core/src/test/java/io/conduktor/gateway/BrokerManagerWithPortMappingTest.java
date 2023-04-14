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


import io.conduktor.gateway.config.*;
import io.conduktor.gateway.config.support.Messages;
import io.conduktor.gateway.network.BrokerManager;
import io.conduktor.gateway.network.BrokerManagerWithPortMapping;
import io.conduktor.gateway.network.GatewayBrokers;
import io.conduktor.gateway.thread.UpStreamResource;
import io.conduktor.gateway.tls.KeyStoreConfig;
import io.netty.channel.nio.NioEventLoopGroup;
import mockit.Mock;
import mockit.MockUp;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.conduktor.gateway.common.NodeUtils.keyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;

class BrokerManagerWithPortMappingTest {

    public static final int BASE_PORT = 6969;
    public static final String GATEWAY_HOST = "localhost";
    public static final String FIRST_HOST = "host3";
    public static final int KAFKA_PORT = 9092;
    public static final String BIND_HOST = "0.0.0.0";
    private static final Properties PROPERTIES = new Properties();
    private static final GatewayBrokers GATEWAY_BROKERS = Mockito.mock(GatewayBrokers.class);
    private static final UpStreamResource UP_STREAM_RESOURCE = Mockito.mock(UpStreamResource.class);
    private static final GatewayConfiguration GATEWAY_CONFIGURATION = Mockito.mock(GatewayConfiguration.class);

    private static final AuthenticationConfig AUTHENTICATION_CONFIG = Mockito.mock(AuthenticationConfig.class);
    private static final SslConfig SSL_CONFIG = Mockito.mock(SslConfig.class);
    private static final KeyStoreConfig KEY_STORE_CONFIG = Mockito.mock(KeyStoreConfig.class);

    private static final HostPortConfiguration HOST_PORT_CONFIGURATION = new HostPortConfiguration();
    private BrokerManagerWithPortMapping brokerManager;

    @BeforeAll
    static void beforeAll() {
        Mockito.when(GATEWAY_BROKERS.getBossGroup()).thenReturn(new NioEventLoopGroup(2));
        PROPERTIES.put(BrokerManager.BOOTSTRAP_SERVERS, keyOf(FIRST_HOST, KAFKA_PORT));
        HOST_PORT_CONFIGURATION.setPortRange("6969:6990");
        HOST_PORT_CONFIGURATION.setGatewayHost(GATEWAY_HOST);
        HOST_PORT_CONFIGURATION.setGatewayBindHost(BIND_HOST);
        var endpoint = new Endpoint(GATEWAY_HOST, BASE_PORT);
        Mockito.when(GATEWAY_CONFIGURATION.getHostPortConfiguration()).thenReturn(HOST_PORT_CONFIGURATION);
        Mockito.when(GATEWAY_CONFIGURATION.getAuthenticationConfig()).thenReturn(AUTHENTICATION_CONFIG);
        Mockito.when(AUTHENTICATION_CONFIG.getAuthenticatorType()).thenReturn(AuthenticatorType.NONE);
        Mockito.when(KEY_STORE_CONFIG.getKeyStorePath()).thenReturn("config/kafka-gateway.keystore.jks");
        Mockito.when(KEY_STORE_CONFIG.getKeyStorePassword()).thenReturn("123456");
        Mockito.when(KEY_STORE_CONFIG.getKeyPassword()).thenReturn("123456");
        Mockito.when(KEY_STORE_CONFIG.getKeyStoreType()).thenReturn("jks");
        Mockito.when(KEY_STORE_CONFIG.getUpdateIntervalMsecs()).thenReturn(600000L);
        Mockito.when(SSL_CONFIG.getKeyStore()).thenReturn(KEY_STORE_CONFIG);
        Mockito.when(SSL_CONFIG.getUpdateContextIntervalMinutes()).thenReturn(5);
        Mockito.when(AUTHENTICATION_CONFIG.getSslConfig()).thenReturn(SSL_CONFIG);
    }

    @BeforeEach
    void setUp() {
        brokerManager = new BrokerManagerWithPortMapping(
                List.of(new Node(-1, FIRST_HOST, KAFKA_PORT)),
                GATEWAY_BROKERS,
                HOST_PORT_CONFIGURATION,
                AUTHENTICATION_CONFIG,
                null);
        brokerManager.setUpstreamResourceAndStartBroker(UP_STREAM_RESOURCE);
        Mockito.clearInvocations(UP_STREAM_RESOURCE);
        Mockito.clearInvocations(GATEWAY_BROKERS);
    }

    @Test
    void testStartBroker_shouldInitFirstMapping() {
        var endpoint = brokerManager.getGatewayByReal(FIRST_HOST, KAFKA_PORT);
        assertThat(endpoint).isNotNull();
        var node = endpoint.getHost();
        assertThat(endpoint.getPort()).isEqualTo(6969);
        assertThat(endpoint.getHost()).isEqualTo("localhost");
    }

    @RepeatedTest(5)
    void testGetMapping_shouldMapPortWithBrokerOrder() {
        var broker1 = initBroker(1);
        var keyHost1 = getKey(broker1);
        var broker2 = initBroker(2);
        var keyHost2 = getKey(broker2);
        var broker3 = initBroker(3);
        var keyHost3 = getKey(broker3);
        var broker4 = initBroker(4);
        var keyHost4 = getKey(broker4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        var mappings = brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
        assertThat(mappings).hasSize(4);
        var port = BASE_PORT;
        assertMapping(mappings.get(keyHost1), GATEWAY_HOST, port);
        assertMapping(mappings.get(keyHost2), GATEWAY_HOST, port + 1);
        assertMapping(mappings.get(keyHost3), GATEWAY_HOST, port + 2);
        assertMapping(mappings.get(keyHost4), GATEWAY_HOST, port + 3);
    }

    @Test
    void testGetMapping_shouldRegisterOnlyNewKafkaNodes() {
        var broker1 = initBroker(1);
        var broker2 = initBroker(2);
        var broker3 = initBroker(3);
        var broker4 = initBroker(4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
        var nodeCaptor = ArgumentCaptor.forClass(Node.class);
        verify(UP_STREAM_RESOURCE, Mockito.times(3)).registerKafkaNode(nodeCaptor.capture());
        var registeredNodes = nodeCaptor.getAllValues();
        assertThat(registeredNodes).hasSize(3)
                .contains(toNode(broker1), toNode(broker2), toNode(broker4));
    }

    @Test
    void testGetMapping_shouldCreateNewGatewayBroker() {
        Mockito.clearInvocations(UP_STREAM_RESOURCE);
        Mockito.clearInvocations(GATEWAY_BROKERS);
        var broker1 = initBroker(1);
        var broker2 = initBroker(2);
        var broker3 = initBroker(3);
        var broker4 = initBroker(4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
        var portCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(GATEWAY_BROKERS, Mockito.times(4)).activateBroker(portCaptor.capture());
        var ports = portCaptor.getAllValues();
        var port = BASE_PORT;
        assertThat(ports).hasSize(4)
                .contains(port++, port++, port++, port);
    }

    @Test
    void testGetMapping_shouldCreateNewGatewayBroker_onlyNewMapping() {
        var broker3 = initBroker(4);
        var broker4 = initBroker(5);
        var broker5 = initBroker(6);
        var responseBrokers = Arrays.asList(broker5, broker3, broker4);
        Collections.shuffle(responseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
        var portCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(GATEWAY_BROKERS, Mockito.times(3)).activateBroker(portCaptor.capture());
        var ports = portCaptor.getAllValues();
        var port = BASE_PORT;
        assertThat(ports).hasSize(3)
                .contains(port++, port++, port);
    }


    @Test
    void testGetMappingWithAddingNewNode() {
        var broker1 = initBroker(1);
        var broker2 = initBroker(2);
        var broker3 = initBroker(3);
        var broker4 = initBroker(4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
    }


    @Test
    void testGetMappingWithRemovingNode_shouldDeregisterNode() {
        Mockito.clearInvocations(UP_STREAM_RESOURCE);
        Mockito.clearInvocations(GATEWAY_BROKERS);
        var broker1 = initBroker(1);
        var broker2 = initBroker(2);
        var broker3 = initBroker(3);
        var broker4 = initBroker(4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
        var broker1a = initBroker(1);
        var broker2a = initBroker(2);
        var broker4a = initBroker(4);
        var newResponseBrokers = Arrays.asList(broker1a, broker2a, broker4a);
        Collections.shuffle(newResponseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(newResponseBrokers.iterator()));
        var nodeCaptor = ArgumentCaptor.forClass(Node.class);
        verify(UP_STREAM_RESOURCE, Mockito.times(1)).deregisterKafkaNode(nodeCaptor.capture());
        var deregisteredNode = nodeCaptor.getValue();
        assertThat(deregisteredNode.host()).isEqualTo(broker3.host());
        assertThat(deregisteredNode.port()).isEqualTo(broker3.port());
    }


    @Test
    void testGetMapping_shouldDoNothingWhenWeHaveUnchangedNodes() {
        var broker1 = initBroker(1);
        var broker2 = initBroker(2);
        var broker3 = initBroker(3);
        var broker4 = initBroker(4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator()));
        Mockito.clearInvocations(UP_STREAM_RESOURCE);
        Mockito.clearInvocations(GATEWAY_BROKERS);
        var broker1a = initBroker(1);
        var broker2a = initBroker(2);
        var broker3a = initBroker(3);
        var broker4a = initBroker(4);
        var newResponseBrokers = Arrays.asList(broker3a, broker1a, broker2a, broker4a);
        Collections.shuffle(newResponseBrokers);
        brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(newResponseBrokers.iterator()));
        verify(UP_STREAM_RESOURCE, Mockito.times(0)).deregisterKafkaNode(any());
        verify(UP_STREAM_RESOURCE, Mockito.times(0)).registerKafkaNode(any());
        verify(GATEWAY_BROKERS, Mockito.times(0)).activateBroker(anyInt());
    }

    @Test
    public void testGateway_shouldShutdown_whenInitBrokerPortMappingNotEnoughPort() {
        var exitCode = new AtomicInteger(0);
        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                exitCode.set(value);
                throw new RuntimeException(String.valueOf(value));
            }
        };

        var hostPortConfiguration = new HostPortConfiguration();
        hostPortConfiguration.setPortRange("6969:6969");
        hostPortConfiguration.setGatewayHost(GATEWAY_HOST);
        hostPortConfiguration.setGatewayBindHost(BIND_HOST);
        Assertions.assertThatThrownBy(() -> new BrokerManagerWithPortMapping(
                        List.of(new Node(-1, FIRST_HOST, KAFKA_PORT), new Node(-1, FIRST_HOST, KAFKA_PORT)),
                        GATEWAY_BROKERS,
                        hostPortConfiguration,
                        AUTHENTICATION_CONFIG,
                        null))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(String.valueOf(Messages.PORT_NOT_ENOUGH_EXIT_CODE));
    }



    @Test
    public void testGateway_shouldShutdown_whenDoesNotHaveEnoughPort() throws IllegalAccessException, NoSuchFieldException {
        var exitCode = new AtomicInteger(0);
        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                exitCode.set(value);
                throw new RuntimeException(String.valueOf(value));
            }
        };
        var brokerManager = new BrokerManagerWithPortMapping(
                List.of(new Node(-1, FIRST_HOST, KAFKA_PORT)),
                GATEWAY_BROKERS,
                HOST_PORT_CONFIGURATION,
                AUTHENTICATION_CONFIG,
                null);
        brokerManager.setUpstreamResourceAndStartBroker(UP_STREAM_RESOURCE);
        var broker1 = initBroker(1);
        var broker2 = initBroker(2);
        var broker3 = initBroker(3);
        var broker4 = initBroker(4);
        var responseBrokers = Arrays.asList(broker1, broker2, broker3, broker4);
        Collections.shuffle(responseBrokers);
        Field gatewayPortsField = BrokerManagerWithPortMapping.class.getDeclaredField("gatewayPorts");
        gatewayPortsField.setAccessible(true);
        gatewayPortsField.set(brokerManager, List.of(1));
        Assertions.assertThatThrownBy(() -> brokerManager.getRealToGatewayMap(new MetadataResponseBrokerCollection(responseBrokers.iterator())))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(String.valueOf(Messages.PORT_NOT_ENOUGH_EXIT_CODE));
    }


    private void assertMapping(Endpoint endpoint, String gatewayHost, int gatewayPort) {
        assertThat(endpoint.getHost()).isEqualTo(gatewayHost);
        assertThat(endpoint.getPort()).isEqualTo(gatewayPort);
    }

    private MetadataResponseData.MetadataResponseBroker initBroker(int nodeId) {
        var broker = new MetadataResponseData.MetadataResponseBroker();
        broker.setHost("host" + nodeId);
        broker.setPort(9092);
        broker.setNodeId(nodeId);
        return broker;
    }

    private Node toNode(MetadataResponseData.MetadataResponseBroker responseBroker) {
        return new Node(responseBroker.nodeId(), responseBroker.host(), responseBroker.port());
    }

    private String getKey(MetadataResponseData.MetadataResponseBroker brokerResponse) {
        return keyOf(brokerResponse.host(), brokerResponse.port());
    }

}
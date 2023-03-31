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

package io.conduktor.gateway.integration.util;

import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

public class SNIKafkaConsumer<K, V> extends KafkaConsumer<K, V> {

    public SNIKafkaConsumer(Map<String, Object> configs) {
        super(configs);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SNIKafkaConsumer(Properties properties) {
        super(properties);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SNIKafkaConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(properties, keyDeserializer, valueDeserializer);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SNIKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void injectCustomHostResolver() throws NoSuchFieldException, IllegalAccessException {
        Field clientField = this.getClass().getSuperclass()
                .getDeclaredField("client");
        clientField.setAccessible(true);
        var consumerNetworkClient = (ConsumerNetworkClient) clientField.get(this);
        clientField = consumerNetworkClient.getClass()
                .getDeclaredField("client");
        clientField.setAccessible(true);
        var networkClient = (NetworkClient) clientField.get(consumerNetworkClient);
        var connectionStatesField = networkClient.getClass()
                .getDeclaredField("connectionStates");
        connectionStatesField.setAccessible(true);
        var connectionState = connectionStatesField.get(networkClient);
        var resolverField = connectionState.getClass()
                .getDeclaredField("hostResolver");
        resolverField.setAccessible(true);
        var localResolver = new CustomHostResolverUtils.LocalhostResolver();
        var hostResolver = resolverField.get(connectionState);
        resolverField.set(connectionState, localResolver);
    }
}

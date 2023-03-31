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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

public class SNIKafkaProducer<K, V> extends KafkaProducer<K, V> {

    public SNIKafkaProducer(Map<String, Object> configs) {
        super(configs);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SNIKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SNIKafkaProducer(Properties properties) {
        super(properties);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SNIKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
        try {
            injectCustomHostResolver();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void injectCustomHostResolver() throws NoSuchFieldException, IllegalAccessException {
        Field senderField = this.getClass().getSuperclass()
                .getDeclaredField("sender");
        senderField.setAccessible(true);
        var sender = senderField.get(this);
        Field clientField = sender.getClass()
                .getDeclaredField("client");
        clientField.setAccessible(true);
        var networkClient = clientField.get(sender);
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

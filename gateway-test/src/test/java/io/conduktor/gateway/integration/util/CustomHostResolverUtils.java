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

import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Inject a default dns resolver to resolve every hostname to localhost
 * otherwise we cannot resolve ip address belongs to hostname
 * use reflection to set it to kafka client
 */
public class CustomHostResolverUtils {


    public static void injectCustomHostResolver(KafkaConsumer consumer) throws NoSuchFieldException, IllegalAccessException {
        Field clientField = consumer.getClass().getSuperclass()
                .getDeclaredField("client");
        clientField.setAccessible(true);
        var consumerNetworkClient = (ConsumerNetworkClient) clientField.get(consumer);
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

    public static void injectCustomHostResolver(KafkaProducer producer) throws NoSuchFieldException, IllegalAccessException {
        Field senderField = producer.getClass()
                .getDeclaredField("sender");
        senderField.setAccessible(true);
        var sender = senderField.get(producer);
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
        var localResolver = new LocalhostResolver();
        var hostResolver = resolverField.get(connectionState);
        resolverField.set(connectionState, localResolver);
    }

    public static void injectCustomHostResolver(KafkaAdminClient adminClient) throws NoSuchFieldException, IllegalAccessException {
        Field clientField = adminClient.getClass()
                .getDeclaredField("client");
        clientField.setAccessible(true);
        var networkClient = (NetworkClient) clientField.get(adminClient);
        var connectionStatesField = networkClient.getClass()
                .getDeclaredField("connectionStates");
        connectionStatesField.setAccessible(true);
        var connectionState = connectionStatesField.get(networkClient);
        var resolverField = connectionState.getClass()
                .getDeclaredField("hostResolver");
        resolverField.setAccessible(true);
        var localResolver = new LocalhostResolver();
        var hostResolver = resolverField.get(connectionState);
        resolverField.set(connectionState, localResolver);
    }

    public static class LocalhostResolver implements HostResolver {

        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            var addr = InetAddress.getByAddress(host, new byte[]{
                    (byte) 127, (byte) 0, (byte) 0, (byte) 1});
            var addresses = new InetAddress[1];
            addresses[0] = addr;
            return addresses;
        }

    }

}

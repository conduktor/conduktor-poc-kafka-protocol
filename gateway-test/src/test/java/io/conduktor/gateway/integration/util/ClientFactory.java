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

import io.conduktor.gateway.DependencyInjector;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.config.kafka.KafkaSelectorConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.conduktor.gateway.integration.util.CustomHostResolverUtils.injectCustomHostResolver;

@Slf4j
public class ClientFactory implements Closeable {

    public Properties gatewayOverrideProperties;
    private GatewayConfiguration gatewayConfiguration;
    private List<AutoCloseable> closeables;
    public Properties kafkaOverrideProperties;

    public ClientFactory(GatewayConfiguration gatewayConfiguration) {
        this.gatewayConfiguration = gatewayConfiguration;
        this.closeables = new ArrayList<>();
        this.gatewayOverrideProperties = new Properties();
        this.kafkaOverrideProperties = new Properties();
    }

    public AdminClient gatewayAdmin() {
        Properties gatewayProperties = getGatewayProperties();
        var adminClient = AdminClient.create(gatewayProperties);
        try {
            injectCustomHostResolver((KafkaAdminClient) adminClient);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        closeables.add(adminClient);
        return adminClient;
    }

    public AdminClient kafkaAdmin() {
        Properties kafkaProperties = getKafkaProperties();
        var adminClient = AdminClient.create(kafkaProperties);
        closeables.add(adminClient);
        return adminClient;
    }

    public KafkaProducer<String, String> gatewayProducer() {
        return producer(getGatewayProperties());
    }

    public KafkaProducer<String, String> kafkaProducer() {
        return producer(getKafkaProperties());
    }

    public KafkaConsumer<String, String> gatewayConsumer(final String groupId) {
        return consumer(getGatewayProperties(), groupId);
    }

    public KafkaConsumer<String, String> gatewayConsumer(final String groupId, final String groupInstanceId) {
        return consumer(getGatewayProperties(), groupId, groupInstanceId);
    }

    public KafkaConsumer<String, String> kafkaConsumer(final String groupId) {
        return consumer(getKafkaProperties(), groupId);
    }

    public void overrideKafkaConfig(KafkaSelectorConfig kafkaConfigPath) {
        this.gatewayConfiguration = gatewayConfiguration
                .withKafkaSelector(kafkaConfigPath);
    }

    public void addGatewayPropertyOverride(String name, String value) {
        gatewayOverrideProperties.put(name, value);
    }

    public void addGatewayPropertyOverrides(Map<String, String> properties) {
        gatewayOverrideProperties.putAll(properties);
    }

    public void addKafkaPropertyOverride(String name, String value) {
        kafkaOverrideProperties.put(name, value);
    }

    public void addKafkaPropertyOverrides(Map<String, String> properties) {
        kafkaOverrideProperties.putAll(properties);
    }

    public void removePropertyOverride(String name) {
        gatewayOverrideProperties.remove(name);
    }

    public void clearPropertyOverrides() {
        gatewayOverrideProperties.clear();
    }

    public Properties getGatewayProperties() {
        Properties clientProperties = new Properties();
        var hostPortConfiguration = gatewayConfiguration.getHostPortConfiguration();
        var ports = hostPortConfiguration.getPortInRange();
        clientProperties.put("bootstrap.servers", hostPortConfiguration.getGatewayHost() + ":" + ports.get(0));
        clientProperties.put("client.id", "testclient001");
        clientProperties.putAll(gatewayOverrideProperties);
        return clientProperties;
    }

    @Override
    public void close() {
        var iterator = closeables.iterator();
        while (iterator.hasNext()) {
            try {
                var closeable = iterator.next();
                closeable.close();
                iterator.remove();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private KafkaProducer<String, String> producer(Properties producerProperties) {
        var producer = new KafkaProducer<String, String>(producerProperties, new StringSerializer(), new StringSerializer());
        closeables.add(producer);
        return producer;
    }

    private KafkaConsumer<String, String> consumer(Properties consumerProperties, final String groupId) {
        final Map<String, Object> config = new HashMap<String, Object>() {{
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }};
        Properties clientProperties = new Properties();
        clientProperties.putAll(consumerProperties);
        clientProperties.putAll(config);
        dumpConfig("Consumer", config);
        var consumer = new SNIKafkaConsumer<>(clientProperties, new StringDeserializer(), new StringDeserializer());
        closeables.add(consumer);
        return consumer;
    }

    private KafkaConsumer<String, String> consumer(Properties consumerProperties, final String groupId, final String groupInstanceId) {
        final Map<String, Object> config = new HashMap<String, Object>() {{
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }};
        Properties clientProperties = new Properties();
        clientProperties.putAll(consumerProperties);
        clientProperties.putAll(config);
        dumpConfig("Consumer", config);
        var consumer = new SNIKafkaConsumer<>(clientProperties, new StringDeserializer(), new StringDeserializer());
        closeables.add(consumer);
        return consumer;
    }

    private Properties getKafkaProperties() {
        // TODO: SASL and SSL versions of this
        Properties kafkaProperties = new Properties();
        try {
            var dependencyInjector = new DependencyInjector(gatewayConfiguration);
            kafkaProperties = dependencyInjector.kafkaPropertiesProvider(gatewayConfiguration).loadKafkaProperties();
            kafkaProperties.putAll(kafkaOverrideProperties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return kafkaProperties;
    }

    private void dumpConfig(final String type, final Map<String, Object> config) {
        log.info("{}:\n{}", type, config.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("\n")));
    }

}

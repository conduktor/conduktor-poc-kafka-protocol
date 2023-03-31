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

package io.conduktor.gateway.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.conduktor.gateway.DependencyInjector;
import io.conduktor.gateway.GatewayExecutor;
import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.config.YamlConfigReader;
import io.conduktor.gateway.config.kafka.KafkaSelectorConfig;
import io.conduktor.gateway.integration.util.ClientFactory;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.network.BrokerManager;
import io.conduktor.gateway.rebuilder.components.RebuildMapper;
import io.conduktor.gateway.service.InterceptorPoolService;
import jakarta.validation.Validator;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.*;
import ru.vyarus.guice.validator.ValidationModule;

import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.conduktor.gateway.integration.util.PortHelper.getContinuousFreePort;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseGatewayIntegrationTest extends DockerComposeIntegrationTest {

    public static final String GATEWAY_CONFIG_FILE = "config/application.yaml";
    private static final String PREFIX_TOPIC = "test_topic_";
    private static final AtomicInteger counter = new AtomicInteger(0);

    protected List<Integer> GATEWAY_PORTS;

    protected ClientFactory clientFactory;
    private GatewayExecutor gateway;

    protected GatewayConfiguration gatewayConfiguration;
    protected Injector injector;
    private RebuildMapper rebuildMapper;
    private MetricsRegistryProvider metricsRegistryProvider;
    @Getter
    private BrokerManager brokerManager;

    @Getter
    private InterceptorPoolService interceptorPoolService;
    @Getter
    // start closed
    private boolean closed = true;

    @BeforeAll
    public void baseInit() {
        startGateway(GATEWAY_CONFIG_FILE);
        clientFactory = new ClientFactory(getGatewayConfiguration());
        reconfigureClientFactory(clientFactory);
    }

    public String getPortRangeForGateway() {
        GATEWAY_PORTS = getContinuousFreePort(2);
        var first = GATEWAY_PORTS.get(0);
        var last = GATEWAY_PORTS.get(1);
        return first + ":" + last;
    }

    public void logPortsGateway() {
        logPortsDocker();
    }

    @BeforeEach()
    public void baseSetup() {
        reconfigureClientFactory(clientFactory);
    }

    @AfterAll
    public void baseDestroy() throws Exception {
        stopGateway();
    }

    @AfterEach
    public void baseTearDown() {
        clientFactory.clearPropertyOverrides();
        clientFactory.close();
    }

    public String createTopic(AdminClient adminClient, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        var topicName = PREFIX_TOPIC + counter.getAndIncrement();
        return createTopic(adminClient, topicName, partitions, replicationFactor);
    }

    public String createTopic(AdminClient adminClient, String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        return createTopic(adminClient, topicName, partitions, replicationFactor, Map.of(), 10);
    }


    public String createTopic(AdminClient adminClient, String topicName, int partitions, short replicationFactor, Map<String, String> configs, int retryTimes) throws ExecutionException, InterruptedException {
        try {
            adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, replicationFactor).configs(configs)))
                    .all()
                    .get();
            var startTime = System.currentTimeMillis();
            long WAITING_TIMEOUT = 5000;
            boolean topicFound = false;
            while (System.currentTimeMillis() < startTime + WAITING_TIMEOUT && !topicFound) {
                topicFound = adminClient.listTopics()
                        .names()
                        .get().contains(topicName);
                if (!topicFound) {
                    Thread.sleep(100);
                }
            }
            if (!topicFound) {
                throw new RuntimeException("Topic " + topicName + " not created in " + WAITING_TIMEOUT + "ms");
            }
            return topicName;
        } catch (Exception exception) {
            if (exception.getCause() instanceof TimeoutException) {
                log.warn("kafka cluster is overloaded. Slow a little bit");
                Thread.sleep(200L);
                return createTopic(adminClient, topicName, partitions, replicationFactor, configs, retryTimes - 1);
            }
            throw new RuntimeException(exception);
        }
    }


    public void deleteTopic(AdminClient adminClient, String topicName, int retryTimes) throws InterruptedException {
        try {
            adminClient.deleteTopics(List.of(topicName))
                    .all()
                    .get();
            var startTime = System.currentTimeMillis();
            long WAITING_TIMEOUT = 5000;
            boolean topicFound = false;
            while (System.currentTimeMillis() < startTime + WAITING_TIMEOUT && !topicFound) {
                topicFound = adminClient.listTopics()
                        .names()
                        .get().contains(topicName);
                if (!topicFound) {
                    Thread.sleep(100);
                }
            }
            if (topicFound) {
                throw new RuntimeException("Topic " + topicName + " not deleted in " + WAITING_TIMEOUT + "ms");
            }
        } catch (Exception exception) {
            if (exception.getCause() instanceof TimeoutException) {
                log.warn("kafka cluster is over load. Slow a little bit");
                Thread.sleep(200L);
                deleteTopic(adminClient, topicName, retryTimes - 1);
            }
            throw new RuntimeException(exception);
        }
    }

    protected int getGatewayPort() {
        return GATEWAY_PORTS.get(0);
    }

    protected void startGateway(String configFile) {
        try {
            generateGatewayConfiguration(configFile);
            reconfigureGateway(gatewayConfiguration);
            gatewayConfiguration.getHostPortConfiguration().setGatewayPort(getGatewayPort());
            this.closed = false;
            startGatewayAtPort(getPortRangeForGateway());
            this.rebuildMapper = injector.getInstance(RebuildMapper.class);
            this.metricsRegistryProvider = injector.getInstance(MetricsRegistryProvider.class);
            this.brokerManager = injector.getInstance(BrokerManager.class);
            this.interceptorPoolService = injector.getInstance(InterceptorPoolService.class);
            log.info("Gateway is started");
        } catch (Exception ex) {
            log.error("Error happen when starting Gatway", ex);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ignored) {
            }
            if (gateway != null) {
                try {
                    gateway.close();
                } catch (Exception e) {
                    log.error("an error occurred when closing Gateway");
                }
            }
            closed = true;
            System.exit(96);
        }
    }

    protected void generateGatewayConfiguration(String configFile) throws IOException {
        YamlConfigReader<GatewayConfiguration> configReader = YamlConfigReader.forType(GatewayConfiguration.class);
        gatewayConfiguration = configReader.readYamlInResources(configFile);
        var hostPortConfiguration = gatewayConfiguration.getHostPortConfiguration();
        hostPortConfiguration.setPortRange(getPortRangeForGateway());
        gatewayConfiguration.setHostPortConfiguration(hostPortConfiguration);
        gatewayConfiguration.getAuthenticationConfig()
                .setAuthenticatorType(getAuthenticatorType());
        gatewayConfiguration.setKafkaSelector(generateKafkaConfig());
    }

    protected void stopGateway() throws Exception {
        log.info("START TO CLOSE GATEWAY");
        if (closed) {
            log.info("GATEWAY ALREADY CLOSED");
            return;
        }
        if (Objects.nonNull(gateway)) {
            gateway.close();
        }
        if (!closed) {
            this.injector = null;
            this.gateway = null;
            this.closed = true;
        }
    }


    protected GatewayConfiguration getGatewayConfiguration() {
        return gatewayConfiguration;
    }

    protected RebuildMapper getRebuildMapper() {
        return rebuildMapper;
    }


    protected MetricsRegistryProvider getMetricsRegistryProvider() {
        return metricsRegistryProvider;
    }

    protected AuthenticatorType getAuthenticatorType() {
        return AuthenticatorType.NONE;
    }

    protected void reconfigureGateway(GatewayConfiguration gatewayConfiguration) {
    }

    protected KafkaSelectorConfig generateKafkaConfig() {
        return configAsTempFile("bootstrap.servers=localhost:" + getKafka1Port());
    }

    protected AbstractModule mockGuiceModule() throws IOException {
        return null;
    }

    protected KafkaSelectorConfig configAsTempFile(String config) {
        try {
            var kafkaConfig = Files.createTempFile(null, "-kafka.config");
            Files.writeString(kafkaConfig, config);
            return new KafkaSelectorConfig.FileConfig(kafkaConfig.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void reconfigureClientFactory(ClientFactory clientFactory) {
        if (Objects.nonNull(clientFactory)) {
            clientFactory.addGatewayPropertyOverride("bootstrap.servers", "localhost:" + getGatewayPort());
            clientFactory.addGatewayPropertyOverride("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            clientFactory.addGatewayPropertyOverride("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            clientFactory.addGatewayPropertyOverride("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            clientFactory.addGatewayPropertyOverride("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            clientFactory.addGatewayPropertyOverride(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            clientFactory.addGatewayPropertyOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
    }

    private void startGatewayAtPort(String portRange) throws Exception {

        gatewayConfiguration.getHostPortConfiguration().setPortRange(portRange);
        log.info("Gateway port: " + gatewayConfiguration.getHostPortConfiguration().getPortRange());
        injector = createInjector();
        var constrainViolations = injector.getInstance(Validator.class).validate(gatewayConfiguration);
        assertThat(constrainViolations).isEmpty();
        try {
            gateway = injector.getInstance(GatewayExecutor.class);
            gateway.start();
            logPortsGateway();
        } catch (Exception exception) {
            log.error("Exception happen when start Gateway", exception);
            if (exception.getCause() instanceof BindException) {
                startGatewayAtPort(getPortRangeForGateway());
            } else {
                closed = true;
            }

        }
    }


    private Injector createInjector() throws IOException {
        var mockInjector = mockGuiceModule();
        var dependencyInjector = new DependencyInjector(gatewayConfiguration);
        if (Objects.isNull(mockInjector)) {
            return Guice.createInjector(new ValidationModule(), dependencyInjector);
        }
        return Guice.createInjector(new ValidationModule(), Modules.override(dependencyInjector).with(mockInjector));
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LoginResult {

        @JsonProperty("username")
        private String username;
        @JsonProperty("roles")
        private String[] roles;
        @JsonProperty("access_token")
        private String accessToken;
        @JsonProperty("token_type")
        private String tokenType;
        @JsonProperty("expires_in")
        private int expiresIn;

    }

}

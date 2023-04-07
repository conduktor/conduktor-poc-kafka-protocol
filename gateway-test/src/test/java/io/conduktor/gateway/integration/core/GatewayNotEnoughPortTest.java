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

package io.conduktor.gateway.integration.core;

import com.google.inject.Guice;
import io.conduktor.gateway.DependencyInjector;
import io.conduktor.gateway.GatewayExecutor;
import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.config.YamlConfigReader;
import io.conduktor.gateway.integration.DockerComposeIntegrationTest;
import io.conduktor.gateway.integration.util.ClientFactory;
import io.conduktor.gateway.network.BrokerManager;
import io.conduktor.gateway.network.BrokerManagerWithPortMapping;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.vyarus.guice.validator.ValidationModule;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.conduktor.gateway.integration.util.KafkaConfigUtils.configAsTempFile;
import static io.conduktor.gateway.integration.util.PortHelper.getContinuousFreePort;
import static io.conduktor.gateway.network.BrokerManagerWithPortMapping.PORT_NOT_ENOUGH_EXIT_CODE;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class GatewayNotEnoughPortTest extends DockerComposeIntegrationTest {

    public static final String GATEWAY_CONFIG_FILE = "config/application.yaml";

    @Test
    public void testStartGateway_shouldFail() throws IOException {
        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                throw new RuntimeException(String.valueOf(value));
            }
        };
        YamlConfigReader<GatewayConfiguration> configReader = YamlConfigReader.forType(GatewayConfiguration.class);
        var gatewayConfiguration = configReader.readYamlInResources(GATEWAY_CONFIG_FILE);
        var hostPortConfiguration = gatewayConfiguration.getHostPortConfiguration();
        hostPortConfiguration.setPortRange("70:70");
        gatewayConfiguration.setHostPortConfiguration(hostPortConfiguration);
        gatewayConfiguration.setKafkaSelector(configAsTempFile("bootstrap.servers=localhost:" + getKafka1Port()));
        var dependencyInjector = new DependencyInjector(gatewayConfiguration);
        var injector = Guice.createInjector(new ValidationModule(), dependencyInjector);
        var constrainViolations = injector.getInstance(Validator.class).validate(gatewayConfiguration);
        assertThat(constrainViolations).isEmpty();
        Assertions.assertThatThrownBy(() -> injector.getInstance(GatewayExecutor.class)).isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("70");
    }


    @Test
    public void testProduceAndConsume() throws IOException, IllegalAccessException, NoSuchFieldException, ExecutionException, InterruptedException {
        var exitCode = new AtomicInteger(0);
        new MockUp<System>() {
            @Mock
            public void exit(int value) {
                exitCode.set(value);
                throw new RuntimeException(String.valueOf(value));
            }
        };
        YamlConfigReader<GatewayConfiguration> configReader = YamlConfigReader.forType(GatewayConfiguration.class);
        var gatewayConfiguration = configReader.readYamlInResources(GATEWAY_CONFIG_FILE);
        var hostPortConfiguration = gatewayConfiguration.getHostPortConfiguration();
        var gatewayPorts = getContinuousFreePort(NUM_OF_BROKER);
        var first = gatewayPorts.get(0);
        var last = gatewayPorts.get(1);
        var portRange = first + ":" + last;
        hostPortConfiguration.setPortRange(portRange);
        hostPortConfiguration.setGatewayHost("localhost");
        gatewayConfiguration.getAuthenticationConfig()
                .setAuthenticatorType(AuthenticatorType.NONE);
        gatewayConfiguration.setHostPortConfiguration(hostPortConfiguration);
        gatewayConfiguration.setKafkaSelector(configAsTempFile("bootstrap.servers=localhost:" + getKafka1Port()));
        var dependencyInjector = new DependencyInjector(gatewayConfiguration);
        var injector = Guice.createInjector(new ValidationModule(), dependencyInjector);
        var constrainViolations = injector.getInstance(Validator.class).validate(gatewayConfiguration);
        assertThat(constrainViolations).isEmpty();
        var gateway = injector.getInstance(GatewayExecutor.class);
        gateway.start();
        var brokerManagerWithPortMapping = injector.getInstance(BrokerManager.class);
        Field gatewayPortsField = BrokerManagerWithPortMapping.class.getDeclaredField("gatewayPorts");
        gatewayPortsField.setAccessible(true);
        gatewayPortsField.set(brokerManagerWithPortMapping, List.of(1));
        var clientFactory = new ClientFactory(gatewayConfiguration);
        try (var adminClient = clientFactory.gatewayAdmin()) {
            adminClient.describeCluster().nodes().get(300L, TimeUnit.MILLISECONDS);
        } catch (Exception exception) {
            Assertions.assertThat(exitCode.get()).isEqualTo(PORT_NOT_ENOUGH_EXIT_CODE);
            Thread.sleep(200L);
        }
    }


}

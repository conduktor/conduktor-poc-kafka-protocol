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
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.config.YamlConfigReader;
import io.conduktor.gateway.integration.DockerComposeIntegrationTest;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.vyarus.guice.validator.ValidationModule;

import java.io.IOException;

import static io.conduktor.gateway.integration.util.KafkaConfigUtils.configAsTempFile;
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




}

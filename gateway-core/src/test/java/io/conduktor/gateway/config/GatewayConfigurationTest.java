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

package io.conduktor.gateway.config;

import io.conduktor.gateway.interceptor.Plugin;
import io.conduktor.gateway.service.InterceptorPoolService;
import io.conduktor.gateway.service.PluginLoader;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GatewayConfigurationTest {

    @Test
    void loadDefaultConfigs() throws Exception {
        try (ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var config = YamlConfigReader.forType(GatewayConfiguration.class).readYamlInResources("application.yaml");
            var validator = validatorFactory.getValidator();
            var constrainViolations = validator.validate(config);
            assertThat(constrainViolations).isEmpty();
            assertThat(config).isNotNull();
            assertThat(config.getKafkaSelector()).isNotNull();
            assertThat(config.getHostPortConfiguration()).isNotNull();
            assertThat(config.getAuthenticationConfig()).isNotNull();
            assertThat(config.getMaxResponseLatency()).isGreaterThan(0);
            assertThat(config.getInFlightRequestExpiryMs()).isGreaterThan(0);
            assertThat(config.getUpstreamConnectionConfig()).isNotNull();
        }
    }

    @Test
    public void testLoadsNestedInterceptorConfig() throws IOException {
        var config = YamlConfigReader.forType(GatewayConfiguration.class).readYamlInResources("interceptors.yaml");
        assertThat(config.getInterceptors().size())
                .isEqualTo(1);
        assertThat(config.getInterceptors().get(0).getConfig().size())
                .isEqualTo(3);
        config.getInterceptors().get(0).getConfig().forEach(configEntry -> {
            if (configEntry.getKey().equals("someKey")) {
                assertThat(configEntry.getValue())
                        .isEqualTo("someValue");
            }
            if (configEntry.getKey().equals("nestedMap")) {
                assertThat((Map<String, String>) configEntry.getValue())
                        .containsExactly(new AbstractMap.SimpleEntry<>("nestedMapKey1", "nestedMapValue1"),
                                new AbstractMap.SimpleEntry<>("nestedMapKey2", "nestedMapValue2"));
            }
            if (configEntry.getKey().equals("nestedList")) {
                assertThat((List<String>) configEntry.getValue())
                        .containsExactly("nestedListValue1", "nestedListValue2");
            }
        });
    }

    @Test
    public void testLoadsInterceptorWithInvalidConfig() throws IOException {
        try (ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var config = YamlConfigReader.forType(GatewayConfiguration.class).readYamlInResources("interceptors.yaml");
            for (var interceptor : config.getInterceptors()) {
                interceptor.setName(null);
                interceptor.setPluginClass(null);
                interceptor.setPriority(-1);
            }
            var validator = validatorFactory.getValidator();
            var constrainViolations = validator.validate(config);
            assertThat(constrainViolations).isNotNull();
            assertThat(constrainViolations.size()).isEqualTo(1);
            assertThat(constrainViolations.iterator().next().getMessage()).isNotNull();
        }
    }

    @Test
    public void testLoadsInterceptorWithDuplicateName() {
        var interceptors = List.of(new InterceptorPluginConfig("name", "className", 1, List.of()),
                new InterceptorPluginConfig("name", "className", 2, List.of()));
        var config = new GatewayConfiguration();
        config.setInterceptors(interceptors);
        assertThatThrownBy(() -> new InterceptorPoolService(config, Collections::emptyList))
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .message()
                .isEqualTo("Interceptor plugin config already exists");
    }
}

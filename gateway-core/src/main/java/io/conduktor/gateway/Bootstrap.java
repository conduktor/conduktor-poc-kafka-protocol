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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.config.YamlConfigReader;
import jakarta.validation.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import ru.vyarus.guice.validator.ValidationModule;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.conduktor.gateway.config.support.Messages.ERROR_READING_CONFIG_FILE;
import static io.conduktor.gateway.config.support.Messages.ERROR_WHEN_CLOSING_GATEWAY;

@Slf4j
public class Bootstrap {

    private final static String CONFIGURATION_FILE_PATH = "application.yaml";
    private final static String CONFIGURATION_FILE_ENVIRONMENT = "CONFIGURATION_FILE_PATH";

    public static void main(String[] args) {
        var injector = initInjector();
        startGateway(injector);
    }

    @SuppressWarnings("unchecked")
    private static GatewayConfiguration loadConfiguration() throws IOException {
        var mapYamlLoader = YamlConfigReader.forType(Map.class);
        Map<String, Object> defaultConfig = mapYamlLoader.readYamlInResources(CONFIGURATION_FILE_PATH);
        Map<String, Object> overrideConfig = Optional.ofNullable(System.getenv(CONFIGURATION_FILE_ENVIRONMENT))
                .map(overrideLocation -> {
                    try {
                        return mapYamlLoader.readYaml(overrideLocation);
                    } catch (IOException e) {
                        throw new RuntimeException("Can't load configuration file", e);
                    }
                })
                .orElseGet(Collections::emptyMap);

        Config conf = ConfigFactory.parseMap(overrideConfig)
                .withFallback(ConfigFactory.parseMap(defaultConfig));

//      Redacted config
        Config redactedConfig = ConfigFactory.parseMap(Map.of(
                        "authenticationConfig", Map.of(
                                "sslConfig", Map.of(
                                        "keyStore", Map.of(
                                                "keyStorePassword", "***",
                                                "keyPassword", "***"
                                        )
                                ),
                                "userPool", conf.getIsNull("authenticationConfig.userPool") ? Collections.emptyList() : conf.getObjectList("authenticationConfig.userPool")
                                        .stream()
                                        .map(ConfigObject::unwrapped)
                                        .peek(userConfig -> userConfig.put("password", "***"))
                                        .toList()
                        )
                )
        ).withFallback(conf);

        log.info("Loaded configuration : {}", new ObjectMapper().writeValueAsString(redactedConfig.root().unwrapped()));

        var mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        return mapper.readValue(mapper.writeValueAsString(conf.root().unwrapped()), GatewayConfiguration.class);
    }

    private static Injector initInjector() {
        try {
            var configuration = loadConfiguration();
            try (ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory()) {
                Validator validator = validatorFactory.getValidator();
                Set<ConstraintViolation<GatewayConfiguration>> violations = validator.validate(configuration);
                if (CollectionUtils.isNotEmpty(violations)) {
                    throw new ValidationException(violations.toString());
                }
            }

            return Guice.createInjector(new ValidationModule(), new DependencyInjector(configuration));
        } catch (Exception ex) {
            log.error("Error happen when read config file", ex);
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException ex1) {
            }
            System.exit(ERROR_READING_CONFIG_FILE);
        }
        return null;
    }

    private static void startGateway(Injector injector) {
        GatewayExecutor app = null;
        try {
            app = injector.getInstance(GatewayExecutor.class);
            app.start();
        } catch (Exception ex) {
            log.error("Error happen when start app", ex);
            if (app != null) {
                try {
                    app.close();
                } catch (Exception e) {
                    log.error("Error happen when close app", e);
                }
            }
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException ex1) {
            }
            System.exit(ERROR_WHEN_CLOSING_GATEWAY);
        }
    }


}

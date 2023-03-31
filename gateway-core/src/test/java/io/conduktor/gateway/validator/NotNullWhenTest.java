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

package io.conduktor.gateway.validator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Guice;
import io.conduktor.gateway.common.JacksonUtils;
import io.conduktor.gateway.config.AuthenticationConfig;
import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.SslConfig;
import jakarta.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.vyarus.guice.validator.ValidationModule;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class NotNullWhenTest {
    private static Validator validator;
    private static AuthenticationConfig validConfig;
    private static SslConfig missingKeyStoreSSLConfig;

    @BeforeAll
    public static void init() throws JsonProcessingException {
        var injector = Guice.createInjector(new ValidationModule());
        validator = injector.getInstance(Validator.class);
        validConfig = JacksonUtils.readTo("""
                {
                    "authenticatorType": "SSL",
                    "sslConfig": {
                        "updateContextIntervalMinutes": 5,
                        "keyStore": {
                            "keyStorePath": "config/kafka-gateway.keystore.jks",
                            "keyStorePassword": "@%one23456",
                            "keyPassword": "12345six@",
                            "keyStoreType": "jks",
                            "updateIntervalMsecs": 600000
                        }
                    }
                }
                """, AuthenticationConfig.class);

        missingKeyStoreSSLConfig = JacksonUtils.readTo("""
                {
                    "keyStore":null,
                    "updateContextIntervalMinutes": 10
                }
                """, SslConfig.class);
    }

    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(new AuthenticationConfig(AuthenticatorType.SSL, null), 1, "sslConfig must not be null"),
                Arguments.of(new AuthenticationConfig(AuthenticatorType.SSL, missingKeyStoreSSLConfig), 1, "sslConfig.keyStore must not be null"),
                Arguments.of(new AuthenticationConfig(AuthenticatorType.NONE, null), 0, null),
                Arguments.of(validConfig, 0, null)
        );
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    public void testValidate(AuthenticationConfig config, int errorNum, String expectedError) {
        var errors = validator.validate(config);
        assertThat(errors).hasSize(errorNum);
        if (errorNum > 0) {
            assertThat(errors.stream().map(i -> i.getPropertyPath() + " " + i.getMessage())).containsOnlyOnce(expectedError);
        }
    }
}

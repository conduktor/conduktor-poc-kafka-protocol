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

package io.conduktor.gateway.config.kafka;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.conduktor.gateway.config.YamlConfigReader;
import lombok.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaSelectorConfigTest {

    @Test
    void invalidConfiguration(@TempDir Path tmpDir) throws IOException {
        var empty = tmpDir.resolve("file.yaml");
        Files.write(empty, """
                """.getBytes());
        assertThrows(MismatchedInputException.class, () ->
                YamlConfigReader.forType(KafkaSelectorConfig.class).readYaml(empty.toAbsolutePath().toString()));

        var invalidSelectorType = tmpDir.resolve("file.yaml");
        Files.write(invalidSelectorType, """
                type: unsupported
                value: kafka:9092
                """.getBytes());
        assertThrows(MismatchedInputException.class, () ->
                YamlConfigReader.forType(KafkaSelectorConfig.class).readYaml(invalidSelectorType.toAbsolutePath().toString()));

        var missingProperty = tmpDir.resolve("file.yaml");
        Files.write(missingProperty, """
                type: file
                wololo: true
                """.getBytes());
        assertThrows(MismatchedInputException.class, () ->
                YamlConfigReader.forType(KafkaSelectorConfig.class).readYaml(missingProperty.toAbsolutePath().toString()));

    }

    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class Config {
        KafkaSelectorConfig selector;
    }
    @Test
    void loadEmbeddedConfiguration(@TempDir Path tmpDir) throws IOException {
        var validConfig = tmpDir.resolve("file.yaml");
        Files.write(validConfig, """
                selector:
                    type: file
                    path: /tmp/configuration
                """.getBytes());
        assertEquals(
                new Config(new KafkaSelectorConfig.FileConfig("/tmp/configuration")),
                YamlConfigReader.forType(Config.class).readYaml(validConfig.toAbsolutePath().toString()));
        var valid2Config = tmpDir.resolve("file.yaml");
        Files.write(valid2Config, """
                selector: {type: file, path: /tmp/configuration}
                """.getBytes());
        assertEquals(
                new Config(new KafkaSelectorConfig.FileConfig("/tmp/configuration")),
                YamlConfigReader.forType(Config.class).readYaml(valid2Config.toAbsolutePath().toString()));
    }

    @Test
    void loadFileYamlConfiguration(@TempDir Path tmpDir) throws IOException {
        var validConfig = tmpDir.resolve("file.yaml");
        Files.write(validConfig, """
                type: file
                path: /tmp/configuration
                """.getBytes());
        assertEquals(
                new KafkaSelectorConfig.FileConfig("/tmp/configuration"),
                YamlConfigReader.forType(KafkaSelectorConfig.class).readYaml(validConfig.toAbsolutePath().toString()));
    }

    @Test
    void loadEnvVarConfigYamlConfiguration(@TempDir Path tmpDir) throws IOException {
        var validConfig = tmpDir.resolve("file.yaml");
        Files.write(validConfig, """
                type: env
                prefix: KAFKA_
                """.getBytes());
        assertEquals(
                new KafkaSelectorConfig.EnvVarConfig("KAFKA_"),
                YamlConfigReader.forType(KafkaSelectorConfig.class).readYaml(validConfig.toAbsolutePath().toString()));
    }
}
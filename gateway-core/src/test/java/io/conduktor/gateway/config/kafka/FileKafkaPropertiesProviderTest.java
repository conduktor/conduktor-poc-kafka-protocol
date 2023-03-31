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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileKafkaPropertiesProviderTest {

    @Test
    void loadKafkaProperties_shouldLoadFromFile(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("kafka.config");
        Files.write(configFile,
                List.of(
                        "bootstrap.servers=kafka1:19092",
                        "send.buffer.bytes=1300000"
                ));

        var provider = new FileKafkaPropertiesProvider(configFile.toAbsolutePath().toString());

        var expected = new Properties() {{
            setProperty("bootstrap.servers", "kafka1:19092");
            setProperty("send.buffer.bytes", "1300000");
        }};
        assertEquals(expected, provider.loadKafkaProperties());
    }

    @Test
    void loadKafkaProperties_shouldFailOnMissingFile(@TempDir Path tempDir) {
        Path configFile = tempDir.resolve("kafka.config");
        var provider = new FileKafkaPropertiesProvider(configFile.toAbsolutePath().toString());

        assertThrows(IOException.class, provider::loadKafkaProperties);
    }
}
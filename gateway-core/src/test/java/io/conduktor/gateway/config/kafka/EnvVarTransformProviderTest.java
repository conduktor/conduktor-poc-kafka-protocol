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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnvVarTransformProviderTest {

    @Data
    @AllArgsConstructor
    static class DummyKafkaProvider implements KafkaPropertiesProvider {
        Properties properties;

        @Override
        public Properties loadKafkaProperties() {
            return properties;
        }

    }

    @Test
    void loadKafkaProperties() throws IOException {
        var originalProvider = new DummyKafkaProvider(new Properties() {{
            setProperty("bootstrap.servers", "${KAFKA_SERVER}:19092");
            setProperty("send.buffer.bytes", "1300000");
        }});
        System.setProperty("KAFKA_SERVER", "kafkabroker");
        var actual = new EnvVarTransformProvider(originalProvider);

        var expected = new Properties() {{
            setProperty("bootstrap.servers", "kafkabroker:19092");
            setProperty("send.buffer.bytes", "1300000");
        }};
        assertEquals(expected, actual.loadKafkaProperties());
    }

}
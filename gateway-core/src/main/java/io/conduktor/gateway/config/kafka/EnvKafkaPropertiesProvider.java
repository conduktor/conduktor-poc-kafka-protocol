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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Properties;

@Slf4j
public record EnvKafkaPropertiesProvider(String prefix) implements KafkaPropertiesProvider {

    @Override
    public Properties loadKafkaProperties() {
        log.info("Loading Kafka Config from environment vars with prefix: " + prefix);
        final Properties cfg = new Properties();
        System.getenv().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .forEach(entry -> {
                    var kafkaKey = entry.getKey().substring(prefix.length())
                            .replace("_", ".")
                            .toLowerCase();

                    // chose ProducerConfig as it has the widest surface
                    if (ProducerConfig.configDef().configKeys().get(kafkaKey) != null &&
                            ProducerConfig.configDef().configKeys().get(kafkaKey).type.equals(ConfigDef.Type.PASSWORD)) {
                        log.info("Adding Kafka config: " + kafkaKey + "=[hidden]");
                    } else {
                        log.info("Adding Kafka config: " + kafkaKey + "=" + entry.getValue());
                    }
                    cfg.put(kafkaKey, entry.getValue());
                });
        return cfg;
    }

}

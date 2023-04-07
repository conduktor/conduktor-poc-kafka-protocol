package io.conduktor.gateway.integration.util;

import io.conduktor.gateway.config.kafka.KafkaSelectorConfig;

import java.nio.file.Files;

public class KafkaConfigUtils {

    public static KafkaSelectorConfig configAsTempFile(String config) {
        try {
            var kafkaConfig = Files.createTempFile(null, "-kafka.config");
            Files.writeString(kafkaConfig, config);
            return new KafkaSelectorConfig.FileConfig(kafkaConfig.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

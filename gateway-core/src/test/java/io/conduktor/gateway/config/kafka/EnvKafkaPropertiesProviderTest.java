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

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvKafkaPropertiesProviderTest {

    @Test
    public void testRedactsPasswords() throws IOException, ReflectiveOperationException {

        updateEnv("KAFKA_SASL_JAAS_CONFIG","something_sensitive");
        updateEnv("KAFKA_BOOTSTRAP_SERVERS","something_not_sensitive");

        // the consumer swallows the exceptions so we have to verify with logs.
        var logStream = new ByteArrayOutputStream();
        var logWriter = new PrintWriter(logStream);
        addAppender(logWriter, "logWriter");

        new EnvKafkaPropertiesProvider("KAFKA_").loadKafkaProperties();

        logWriter.flush();

        assertThat(new String(logStream.toByteArray())
                .contains("something_sensitive"))
                .isEqualTo(false);
        assertThat(new String(logStream.toByteArray())
                .contains("something_not_sensitive"))
                .isEqualTo(true);
    }

    @SuppressWarnings({ "unchecked" })
    public static void updateEnv(String name, String val) throws ReflectiveOperationException {
        Map<String, String> env = System.getenv();
        Field field = env.getClass().getDeclaredField("m");
        field.setAccessible(true);
        ((Map<String, String>) field.get(env)).put(name, val);
    }

    void addAppender(final Writer writer, final String writerName) {
        final LoggerContext context = LoggerContext.getContext(false);
        final Configuration config = context.getConfiguration();
        final PatternLayout layout = PatternLayout.createDefaultLayout(config);
        final Appender appender = WriterAppender.createAppender(layout, null, writer, writerName, false, true);
        appender.start();
        config.addAppender(appender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(appender, null, null);
        }
        config.getRootLogger().addAppender(appender, null, null);
    }
}

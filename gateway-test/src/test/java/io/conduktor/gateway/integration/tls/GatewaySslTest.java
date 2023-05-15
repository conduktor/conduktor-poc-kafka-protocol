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

package io.conduktor.gateway.integration.tls;

import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.ClientFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class GatewaySslTest extends BaseGatewayIntegrationTest {

    @Override
    protected AuthenticatorType getAuthenticatorType() {
        return AuthenticatorType.SSL;
    }

    @Override
    protected void reconfigureGateway(GatewayConfiguration gatewayConfiguration) {
        gatewayConfiguration.getAuthenticationConfig().setAuthenticatorType(AuthenticatorType.SSL);
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyStorePath("config/tls/ssl_only/keystore.jks");
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyStorePassword("changeit");
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyPassword("changeit");
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyStoreType("pkcs12");
    }

    @Override
    protected void reconfigureClientFactory(ClientFactory clientFactory) {
        clientFactory.addGatewayPropertyOverride("bootstrap.servers", "localhost:" + getGatewayPort());
        clientFactory.addGatewayPropertyOverride("security.protocol", "SSL");
        clientFactory.addGatewayPropertyOverride("ssl.truststore.location", "config/tls/ssl_only/truststore.jks");
        clientFactory.addGatewayPropertyOverride("ssl.truststore.password", "changeit");
        clientFactory.addGatewayPropertyOverride(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientFactory.addGatewayPropertyOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    @Test
    @Tag("IntegrationTest")
    public void testConnectionOnSSlOnlyGateway() throws ExecutionException, InterruptedException {
        var client = clientFactory.gatewayAdmin();
        var topic = createTopic(client, 1, (short) 1);

        try (var gatewayProducer = clientFactory.gatewayProducer()) {
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "KEY_test", "SSL FTW");

            var recordMetadata = gatewayProducer.send(record1).get();

            assertThat(recordMetadata.hasOffset())
                    .isTrue();
        }

        try (var kafkaConsumer = clientFactory.gatewayConsumer(GatewaySslTest.class.getName() + "testConnectionOnSSlOnlyGateway")) {
            kafkaConsumer.assign(Collections.singletonList(new TopicPartition(topic, 0)));
            long startTime = System.currentTimeMillis();
            int recordCount = 0;
            ConsumerRecords<String, String> records = null;
            while (recordCount != 1 && System.currentTimeMillis() < startTime + 10000L) {
                records = kafkaConsumer.poll(Duration.of(2, ChronoUnit.SECONDS));
                recordCount = records.count();
            }

            assertThat(records).isNotNull();
            assertThat(records.count())
                    .isEqualTo(1);
            var record = records.iterator().next();
            assertThat(record.key())
                    .isEqualTo("KEY_test");
            assertThat(record.value())
                    .isEqualTo("SSL FTW");
        }
    }


}

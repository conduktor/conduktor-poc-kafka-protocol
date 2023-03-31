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

package io.conduktor.gateway.integration;

import io.conduktor.gateway.integration.util.PortHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

import java.io.File;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class DockerComposeIntegrationTest {

    private static final String DOCKER_COMPOSE_FILE_PATH = "/docker-compose.yaml";
    private static final int MINIO_PORT = 9000;
    private static final AtomicBoolean isStarted = new AtomicBoolean(false);

    private static final int ZK_PORT = PortHelper.getFreePort();
    private static final int KAFKA1_PORT = PortHelper.getFreePort();
    private static final int KAFKA1_SASL_PORT = PortHelper.getFreePort();
    private static final int KAFKA2_PORT = PortHelper.getFreePort();
    private static final int KAFKA2_SASL_PORT = PortHelper.getFreePort();
    private static final int PLATFORM_PORT = PortHelper.getFreePort();
    private static final int VAULT_PORT = 8200;
    private static final int AUTH_PORT = PortHelper.getFreePort();

    private static int GATEWAY_PORT;

    private static String CP_VERSION = "latest";
    @SuppressWarnings("rawtypes")
    protected static DockerComposeContainer dockerComposeContainer;

    @SuppressWarnings("rawtypes")
    @BeforeAll
    public static void setupEnvironments() {
        updateGatewayPort();
        if (isStarted.get()) {
            return;
        }
        PortHelper.registerPort(VAULT_PORT);
        var dockerFile = new File(Objects.requireNonNull(DockerComposeIntegrationTest.class.getResource(DOCKER_COMPOSE_FILE_PATH)).getFile());
        var cpVersion = System.getenv("CP_VERSION");
        if (StringUtils.isNotEmpty(cpVersion)) {
            CP_VERSION = cpVersion;
        }
        log.info("Running with CP version: " + CP_VERSION);
        dockerComposeContainer = new DockerComposeContainer(dockerFile)
                .withEnv("CP_VERSION", String.valueOf(CP_VERSION))
                .withEnv("ZK_PORT", String.valueOf(ZK_PORT))
                .withEnv("KAFKA1_PORT", String.valueOf(KAFKA1_PORT))
                .withEnv("KAFKA2_PORT", String.valueOf(KAFKA2_PORT))
                .withEnv("KAFKA1_SASL_PORT", String.valueOf(KAFKA1_SASL_PORT))
                .withEnv("KAFKA2_SASL_PORT", String.valueOf(KAFKA2_SASL_PORT))
                .withLocalCompose(true)
                .withTailChildContainers(true)
                .waitingFor("kafka1", new DockerComposeIntegrationTest.KafkaTopicsWaitStrategy(9092))
                .waitingFor("kafka2", new DockerComposeIntegrationTest.KafkaTopicsWaitStrategy(9093))
                .withEnv("AUTH_PORT", String.valueOf(AUTH_PORT))
                .withEnv("VAULT_PORT", String.valueOf(VAULT_PORT))
                .withEnv("MINIO_PORT", String.valueOf(MINIO_PORT))
                .withEnv("PLATFORM_PORT", String.valueOf(PLATFORM_PORT));
        try {
            dockerComposeContainer.start();
        } catch (Throwable e) {
            Assertions.fail("Error starting compose", e);
        }
        isStarted.set(true);

    }

    protected static void updateGatewayPort() {
        GATEWAY_PORT = PortHelper.getFreePort();
    }


    public static void logPortsDocker() {
        log.debug("Auth service started on: " + AUTH_PORT);
        log.debug("Kafka1 service started on: " + KAFKA1_PORT);
        log.debug("Kafka2 service started on: " + KAFKA2_PORT);
        log.debug("Platform service started on: " + PLATFORM_PORT);
        log.debug("Zookeeper service started on: " + ZK_PORT);
    }

    protected int getKafka1Port() {
        return KAFKA1_PORT;
    }



    public static class KafkaTopicsWaitStrategy implements WaitStrategy {

        private final int listenerPort;
        private Duration timeout = Duration.ofSeconds(60);

        public KafkaTopicsWaitStrategy(int listenerPort) {
            this.listenerPort = listenerPort;
        }

        @Override
        public void waitUntilReady(WaitStrategyTarget target) {
            long startTime = System.currentTimeMillis();
            while (true) {
                try {
                    if (target.execInContainer(
                            "timeout",
                            "10",
                            "kafka-topics",
                            "--bootstrap-server",
                            "localhost:" + listenerPort,
                            "--list").getExitCode() == 0) {
                        break;
                    }
                } catch (Exception e) {
                    // ignored
                    break;
                }
                if (System.currentTimeMillis() > startTime + timeout.toMillis()) {
                    break;
                }
            }
        }

        @Override
        public WaitStrategy withStartupTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

    }


}

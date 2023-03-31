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

package io.conduktor.gateway.integration.metrics;

import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class JvmThreadMetricsTest extends BaseGatewayIntegrationTest {
    @Test
    void threadMetrics() {
        var registry = getMetricsRegistryProvider().registry();
        new JvmThreadMetrics().bindTo(registry);

        assertThat(registry.get("jvm.threads.live").gauge().value()).isPositive();
        assertThat(registry.get("jvm.threads.daemon").gauge().value()).isPositive();
        assertThat(registry.get("jvm.threads.peak").gauge().value()).isPositive();
        assertThat(registry.get("jvm.threads.states").tag("state", "runnable").gauge().value()).isPositive();

        createBlockedThread();
        assertThat(registry.get("jvm.threads.states").tag("state", "blocked").gauge().value()).isPositive();
        assertThat(registry.get("jvm.threads.states").tag("state", "waiting").gauge().value()).isPositive();

        createTimedWaitingThread();
        assertThat(registry.get("jvm.threads.states").tag("state", "timed-waiting").gauge().value()).isPositive();
    }

    private void createTimedWaitingThread() {
        new Thread(() -> {
            sleep(5);
        }).start();
        sleep(1);
    }

    private void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ignored) {
        }
    }

    private void createBlockedThread() {
        var lock = new Object();
        new Thread(() -> {
            synchronized (lock) {
                sleep(5);
            }
        }).start();
        new Thread(() -> {
            synchronized (lock) {
                sleep(5);
            }
        }).start();
        sleep(1);
    }
}

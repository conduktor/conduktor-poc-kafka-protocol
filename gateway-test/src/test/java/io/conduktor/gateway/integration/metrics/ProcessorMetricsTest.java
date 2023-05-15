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
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorMetricsTest extends BaseGatewayIntegrationTest {

    private MeterRegistry registry;

    @BeforeAll
    void beforeAll() {
        registry = getMetricsRegistryProvider().registry();
    }

    @Test
    @Tag("IntegrationTest")
    void cpuMetrics() {
        assertThat(registry.get("system.cpu.count").gauge().value()).isPositive();
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            assertThat(registry.find("system.load.average.1m").gauge()).describedAs("Not present on windows").isNull();
        } else {
            assertThat(registry.get("system.load.average.1m").gauge().value()).isNotNegative();
        }
    }

    @Test
    @Tag("IntegrationTest")
    void hotspotCpuMetrics() {

        assertThat(registry.get("system.cpu.usage").gauge().value()).isNotNegative();
        assertThat(registry.get("process.cpu.usage").gauge().value()).isNotNegative();
    }

}

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
import io.micrometer.core.instrument.binder.BaseUnits;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JvmMemoryMetricsTest extends BaseGatewayIntegrationTest {

    @Test
    @Tag("IntegrationTest")
    void memoryMetrics() {
        var registry = getMetricsRegistryProvider().registry();

        assertJvmBufferMetrics(registry, "direct");
        assertJvmBufferMetrics(registry, "mapped");

        assertJvmMemoryMetrics(registry, "heap");
        assertJvmMemoryMetrics(registry, "nonheap");
    }

    private void assertJvmMemoryMetrics(MeterRegistry registry, String area) {
        var memUsed = registry.get("jvm.memory.used").tags("area", area).gauge();
        assertThat(memUsed.value()).isNotNegative();
        assertThat(memUsed.getId().getBaseUnit()).isEqualTo(BaseUnits.BYTES);

        var memCommitted = registry.get("jvm.memory.committed").tags("area", area).gauge();
        assertThat(memCommitted.value()).isNotNull();
        assertThat(memCommitted.getId().getBaseUnit()).isEqualTo(BaseUnits.BYTES);

        var memMax = registry.get("jvm.memory.max").tags("area", area).gauge();
        assertThat(memMax.value()).isNotNull();
        assertThat(memMax.getId().getBaseUnit()).isEqualTo(BaseUnits.BYTES);
    }

    private void assertJvmBufferMetrics(MeterRegistry registry, String bufferId) {
        assertThat(registry.get("jvm.buffer.count").tags("id", bufferId).gauge().value()).isNotNegative();

        var memoryUsedDirect = registry.get("jvm.buffer.memory.used").tags("id", bufferId).gauge();
        assertThat(memoryUsedDirect.value()).isNotNull();
        assertThat(memoryUsedDirect.getId().getBaseUnit()).isEqualTo(BaseUnits.BYTES);

        var bufferTotal = registry.get("jvm.buffer.total.capacity").tags("id", bufferId).gauge();
        assertThat(bufferTotal.value()).isNotNegative();
        assertThat(bufferTotal.getId().getBaseUnit()).isEqualTo(BaseUnits.BYTES);
    }
}

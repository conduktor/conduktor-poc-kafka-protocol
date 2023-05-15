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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClassLoaderMetricsTest extends BaseGatewayIntegrationTest {

    @Test
    @Tag("IntegrationTest")
    void classLoadingMetrics() {
        assertThat(getMetricsRegistryProvider().registry()
                .get("jvm.classes.loaded").gauge().value()).isPositive();
    }
}

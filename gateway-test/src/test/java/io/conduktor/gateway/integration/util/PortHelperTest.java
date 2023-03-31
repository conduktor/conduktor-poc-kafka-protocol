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

package io.conduktor.gateway.integration.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortHelperTest {

    @Test
    void getContinuousFreePort() {
        var port = PortHelper.getContinuousFreePort(1);
        assertThat(port).hasSize(1);
        assertThat(PortHelper.isOSFreePort(port.get(0))).isTrue();

        var ports = PortHelper.getContinuousFreePort(5);
        assertThat(ports).hasSize(5);
        assertEquals((int) ports.get(0), ports.get(1) - 1);
        assertEquals((int) ports.get(0), ports.get(2) - 2);
        assertEquals((int) ports.get(0), ports.get(3) - 3);
        assertEquals((int) ports.get(0), ports.get(4) - 4);
        assertTrue(PortHelper.isOSFreePort(ports.get(0)));
        assertTrue(PortHelper.isOSFreePort(ports.get(1)));
        assertTrue(PortHelper.isOSFreePort(ports.get(2)));
        assertTrue(PortHelper.isOSFreePort(ports.get(3)));
        assertTrue(PortHelper.isOSFreePort(ports.get(4)));
    }
}
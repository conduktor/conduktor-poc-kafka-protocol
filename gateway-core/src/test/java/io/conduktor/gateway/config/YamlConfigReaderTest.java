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

package io.conduktor.gateway.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class YamlConfigReaderTest {

    @Test
    void loadWithoutEnv() throws Exception {
        var config = YamlConfigReader.forType(YamlDataConfig.class).readYaml("src/test/resources/conf.yaml");
        assertEquals("this is global value", config.global);
        assertEquals("company name", config.company.name);
        assertEquals("abc", config.company.address.street);
        assertEquals(123456, config.company.address.phone);
        assertEquals(2, config.company.staff.size());
        assertEquals("abc", config.company.staff.get(0).name);
        assertEquals(12, config.company.staff.get(0).age);
        assertEquals("xyz", config.company.staff.get(1).name);
        assertEquals(120, config.company.staff.get(1).age);
        assertEquals("localhost", config.endpoint.getHost());
        assertEquals(80, config.endpoint.getPort());
        assertEquals(null, config.empty);
    }

    @Test
    void loadWithEnv() throws Exception {
        System.setProperty("G_VALUE", "xxx");
        System.setProperty("COMPANY_NAME", "dfkjasdlf");
        System.setProperty("EMPTY", "~");
        var config = YamlConfigReader.forType(YamlDataConfig.class).readYaml("src/test/resources/configenv.yaml");
        assertEquals(System.getProperty("G_VALUE"), config.global);
        assertEquals(System.getProperty("COMPANY_NAME"), config.company.name);
        assertEquals(1234, config.company.address.phone);
        assertEquals("abc", config.company.address.street);
        assertEquals(2, config.company.staff.size());
        assertEquals("abc", config.company.staff.get(0).name);
        assertEquals(12, config.company.staff.get(0).age);
        assertEquals("xyz", config.company.staff.get(1).name);
        assertEquals(120, config.company.staff.get(1).age);
        assertEquals(null, config.empty);

    }

}
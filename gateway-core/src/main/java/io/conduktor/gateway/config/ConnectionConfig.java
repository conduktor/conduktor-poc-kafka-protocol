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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ConnectionConfig {

    private int numOfConnection = 10;
    private int maxIdleTimeMs = 200_000;

    public void setNumOfConnection(int numOfConnection) {
        this.numOfConnection = Math.max(1, numOfConnection);
    }

    public void setMaxIdleTimeMs(int maxIdleTimeMs) {
        this.maxIdleTimeMs = Math.max(0, maxIdleTimeMs);
    }
}

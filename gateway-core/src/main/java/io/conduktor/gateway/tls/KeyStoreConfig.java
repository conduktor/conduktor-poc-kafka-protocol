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

package io.conduktor.gateway.tls;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class KeyStoreConfig {

    @NotBlank
    private String keyStorePath;
    @NotBlank
    private String keyStorePassword;
    @NotBlank
    private String keyPassword;
    @NotBlank
    private String keyStoreType = "jks";
    @Min(1)
    @Max(Integer.MAX_VALUE)
    private long updateIntervalMsecs = 600000L;

    public void setUpdateIntervalMsecs(long updateIntervalMsecs) {
        this.updateIntervalMsecs = Math.max(1, updateIntervalMsecs);
    }

}

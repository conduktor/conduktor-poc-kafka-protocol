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

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class InterceptorPluginConfig {

    @NotBlank
    private String name;
    @NotBlank
    private String pluginClass;
    @Min(1)
    @Max(Integer.MAX_VALUE)
    private int priority = Integer.MAX_VALUE;
    @Min(0)
    @Max(Long.MAX_VALUE)
    private Long timeoutMs;
    @Valid
    @NotNull
    private List<InterceptorConfigEntry> config;

    public InterceptorPluginConfig(String name, String pluginClass, int priority, List<InterceptorConfigEntry> config) {
        this.name = name;
        this.pluginClass = pluginClass;
        this.priority = priority;
        this.config = config;
    }
}

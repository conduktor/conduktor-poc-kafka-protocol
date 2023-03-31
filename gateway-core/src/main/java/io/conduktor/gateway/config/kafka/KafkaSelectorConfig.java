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

package io.conduktor.gateway.config.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KafkaSelectorConfig.FileConfig.class, name = "file"),
        @JsonSubTypes.Type(value = KafkaSelectorConfig.EnvVarConfig.class, name = "env")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public sealed interface KafkaSelectorConfig {
    record FileConfig(@JsonProperty(required= true) String path) implements KafkaSelectorConfig {
    }

    record EnvVarConfig(@JsonProperty(required= true) String prefix) implements KafkaSelectorConfig {
    }
}

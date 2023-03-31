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

import io.conduktor.gateway.config.kafka.KafkaSelectorConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.Collections;
import java.util.List;

@ToString
@AllArgsConstructor
@Getter
@Setter
@With
public class GatewayConfiguration {

    @NotNull
    private KafkaSelectorConfig kafkaSelector;
    @NotNull
    @Valid
    private HostPortConfiguration hostPortConfiguration;
    private String routing;
    @NotNull
    @Valid
    private AuthenticationConfig authenticationConfig;
    @NotNull
    private ThreadConfig threadConfig;
    private int maxResponseLatency;

    private ConnectionConfig upstreamConnectionConfig;

    private long inFlightRequestExpiryMs;

    private GaugeBackendBrokersTimerConfig gaugeBackendBrokersTimerConfig;

    @Valid
    private List<InterceptorPluginConfig> interceptors;

    public GatewayConfiguration() {
        this.threadConfig = new ThreadConfig();
        this.maxResponseLatency = 6_000;
        this.upstreamConnectionConfig = new ConnectionConfig();
        this.inFlightRequestExpiryMs = 30000; //30s;
        this.authenticationConfig = new AuthenticationConfig();
        this.gaugeBackendBrokersTimerConfig = new GaugeBackendBrokersTimerConfig();
        this.interceptors = Collections.emptyList();
    }

}

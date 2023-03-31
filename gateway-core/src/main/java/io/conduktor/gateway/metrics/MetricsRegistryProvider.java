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

package io.conduktor.gateway.metrics;

import io.conduktor.gateway.model.User;
import io.conduktor.gateway.network.GatewayChannel;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MetricsRegistryProvider implements Closeable {

    private static final User DUMMY_USER = new User("Conduktor");
    @Getter
    private final MeterRegistry meterRegistry;
    @Getter
    private final Counter downstreamCounter;
    @Getter
    private final Counter upstreamCounter;
    private final ConcurrentHashMap<User, Queue<GatewayChannel>> channelsByUser = new ConcurrentHashMap<>();

    public MetricsRegistryProvider(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        meterRegistry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(@NonNull Meter.Id id,@NonNull DistributionStatisticConfig config) {
                if (id.getName().startsWith(MetricsRegistryKeys.REQUEST_RESPONSE_LATENCY)) {
                    return DistributionStatisticConfig.builder()
                            .percentiles(0.3, 0.5, 0.95, 0.99)
                            .percentilesHistogram(true)
                            .build()
                            .merge(config);
                }
                return config;
            }
        });
        this.upstreamCounter = registry().counter(MetricsRegistryKeys.BYTES_EXCHANGED, Tags.of("direction", "upstream"));
        this.downstreamCounter = registry().counter(MetricsRegistryKeys.BYTES_EXCHANGED, Tags.of("direction", "downstream"));
    }

    public MeterRegistry registry() {
        return meterRegistry;
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(meterRegistry)) {
            meterRegistry.close();
        }
    }

    public void trackGatewayChannel(GatewayChannel gatewayChannel) {
        if (!gatewayChannel.getAuthenticator().complete()) {
            return;
        }
        var user = DUMMY_USER;
        var userChannels = channelsByUser.computeIfAbsent(user, u -> new ConcurrentLinkedQueue<>());
        userChannels.add(gatewayChannel);
        gatewayChannel.getGatewaySocketChannel().closeFuture().addListener(future -> channelsByUser.compute(user, (usr, gatewayChannelQueue) -> {
            if (CollectionUtils.isNotEmpty(gatewayChannelQueue)) {
                gatewayChannelQueue.remove(gatewayChannel);
            }
            if (gatewayChannelQueue != null && gatewayChannelQueue.isEmpty()) {
                channelsByUser.remove(user);
                return null;
            }
            return gatewayChannelQueue;
        }));
    }

    public Timer globalTimer() {
        return registry().timer(MetricsRegistryKeys.REQUEST_RESPONSE_LATENCY);
    }



}

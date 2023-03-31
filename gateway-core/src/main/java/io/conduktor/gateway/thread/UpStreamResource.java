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

package io.conduktor.gateway.thread;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.conduktor.gateway.config.ConnectionConfig;
import io.conduktor.gateway.config.UpstreamThreadConfig;
import io.conduktor.gateway.error.handler.ErrorHandler;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.rebuilder.components.RebuildMapper;
import io.conduktor.gateway.service.InFlightRequestService;
import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import org.apache.kafka.common.Node;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static io.conduktor.gateway.metrics.MetricsRegistryKeys.UPSTREAM_NODES;

/**
 * simple thread group to manage {@link GatewayThread}
 */
public class UpStreamResource extends MultithreadEventLoopGroup {

    private final AtomicInteger nodeCount;

    @Inject
    public UpStreamResource(@Named("upstreamThreadConfig") UpstreamThreadConfig upstreamThreadConfig,
                            @Named("kafkaServerProperties") Properties selectorProps,
                            @Named("upStreamConnectionConfig") ConnectionConfig connectionConfig,
                            RebuildMapper rebuildMapper,
                            InFlightRequestService inFlightRequestService,
                            ErrorHandler errorHandler,
                            MetricsRegistryProvider metricsRegistryProvider) {
        super(upstreamThreadConfig.getNumberOfThread(), new ThreadPerTaskExecutor(new DefaultThreadFactory(UpStreamResource.class)),
                selectorProps.clone(), connectionConfig,
                rebuildMapper,
                inFlightRequestService, errorHandler, metricsRegistryProvider, upstreamThreadConfig);
        this.nodeCount = metricsRegistryProvider.registry().gauge(UPSTREAM_NODES, new AtomicInteger(0));
    }
    @Override
    protected EventLoop newChild(Executor executor, Object... args) {
        var selectorProps = (Properties) args[0];
        var connectionConfig = (ConnectionConfig) args[1];
        var rebuildMapper = (RebuildMapper) args[2];
        var inFlightRequestService = (InFlightRequestService) args[3];
        var errorHandler = (ErrorHandler) args[4];
        var metricsRegistryProvider = (MetricsRegistryProvider) args[5];
        var upstreamThreadConfig = (UpstreamThreadConfig) args[6];

        return new GatewayThread(this,
                executor,
                RejectedExecutionHandlers.reject(),
                rebuildMapper,
                selectorProps,
                connectionConfig,
                upstreamThreadConfig.getMaxPendingTask(),
                inFlightRequestService,
                errorHandler,
                metricsRegistryProvider
        );
    }

    public void registerKafkaNode(Node node) {
        nodeCount.getAndIncrement();
        for (io.netty.util.concurrent.EventExecutor eventExecutor : this) {
            ((GatewayThread) eventExecutor).registerNode(node);
        }
    }

    public void deregisterKafkaNode(Node node) {
        nodeCount.getAndDecrement();
        for (io.netty.util.concurrent.EventExecutor eventExecutor : this) {
            ((GatewayThread) eventExecutor).deregisterKafkaNode(node);
        }
    }

}

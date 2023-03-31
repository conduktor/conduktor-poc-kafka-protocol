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

public interface MetricsRegistryKeys {
    /**
     * Connections in GatewayBroker to upstream Kafka instances.
     */
    String BROKERED_ACTIVE_CONNECTIONS = "gateway.brokered_active_connections";
    String REQUEST_RESPONSE_LATENCY = "gateway.latency.request_response";
    String BYTES_EXCHANGED = "gateway.bytes_exchanged";
    String THREAD_REBUILD_REQUEST = "gateway.thread.request.rebuild";
    String THREAD_RECEIVED_REQUEST = "gateway.thread.request.received";
    String THREAD_TASKS = "gateway.thread.tasks";

    String UPSTREAMIO_CONNECTIONS_UPSTREAM_CONNECTED = "gateway.upstreamio.connections.upstream.connected";
    String UPSTREAMIO_CONNECTIONS_DOWNSTREAM = "gateway.upstreamio.connections.downstream";
    String UPSTREAMIO_NODES = "gateway.upstreamio.nodes";
    String UPSTREAM_NODES = "gateway.upstream.nodes";
}

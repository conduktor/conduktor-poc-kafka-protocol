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

package io.conduktor.gateway.network;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Getter
@ToString
public class UpStreamConnection {

    private final String connectionId;
    private final Node node;
    private final ConcurrentHashMap<GatewayChannel, Integer> associatedChannels = new ConcurrentHashMap<>();

    public UpStreamConnection(String connectionId, Node node) {
        this.connectionId = connectionId;
        this.node = node;
    }

    public void trackDownStreamConnection(GatewayChannel gatewayChannel) {
        associatedChannels.put(gatewayChannel, 1);
        gatewayChannel.closeFuture().thenAccept(rs -> associatedChannels.remove(gatewayChannel));
    }

    public void disconnect() {
        associatedChannels.forEach((c, v) -> {
            try {
                log.debug("Close the connection {} because associated connection is closed: {}", c, associatedChannels);
                c.close();
            } catch (IOException e) {
                log.error("error happen when close down stream connection", e);
                throw new RuntimeException(e);
            }
        });
        associatedChannels.clear();
    }

}

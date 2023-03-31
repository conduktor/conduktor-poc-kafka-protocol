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
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.common.Node;

import java.util.Objects;

@Getter
@ToString
@AllArgsConstructor
public class GatewayPortAndKafkaNodePair {

    @Setter
    private int gatewayPort;
    private Node realClusterNode;

    @Override
    public int hashCode() {
        return Objects.hash(realClusterNode.host(), realClusterNode.port(), gatewayPort);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GatewayPortAndKafkaNodePair that = (GatewayPortAndKafkaNodePair) o;
        return Objects.equals(realClusterNode.host(), that.realClusterNode.host())
                && Objects.equals(realClusterNode.port(), that.realClusterNode.port())
                && Objects.equals(gatewayPort, that.gatewayPort);
    }

}

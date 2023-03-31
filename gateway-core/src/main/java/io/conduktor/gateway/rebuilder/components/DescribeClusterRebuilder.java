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

package io.conduktor.gateway.rebuilder.components;

import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.RebuilderTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.conduktor.gateway.common.NodeUtils.keyOf;

@Slf4j
public class DescribeClusterRebuilder extends AbstractReBuilder<DescribeClusterRequest, DescribeClusterResponse>{

    public DescribeClusterRebuilder(RebuilderTools rebuilderTools) {
        super(ApiKeys.DESCRIBE_CLUSTER, rebuilderTools);

    }

    @Override
    public CompletionStage<DescribeClusterResponse> rebuildResponse(DescribeClusterResponse response, ClientRequest clientRequest) {
        var mappings = rebuilderTools.brokerManager().getRealToGatewayMap(response.data().brokers());
        var brokerIter = response.data().brokers().iterator();
        while (brokerIter.hasNext()) {
            var b = brokerIter.next();
            var mapping = mappings.get(keyOf(b.host(), b.port()));
            if (mapping == null) {
                log.error("Unknown broker node seen in {}: {}:{}", ApiKeys.DESCRIBE_CLUSTER, b.host(), b.port());
                // we remove this response from the list sent back
                brokerIter.remove();
            } else {
                log.debug(
                        "Rewriting {}: {}:{} -> {}:{}",
                        ApiKeys.DESCRIBE_CLUSTER,
                        b.host(),
                        b.port(),
                        mapping.getHost(),
                        mapping.getPort()
                );
                b.setHost(mapping.getHost());
                b.setPort(mapping.getPort());
            }
        }
        return CompletableFuture.completedFuture(response);
    }
}

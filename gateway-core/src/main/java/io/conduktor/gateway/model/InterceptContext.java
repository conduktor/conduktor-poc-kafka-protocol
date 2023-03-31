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

package io.conduktor.gateway.model;

import io.conduktor.gateway.interceptor.DirectionType;
import io.conduktor.gateway.service.ClientRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.List;

@Getter
@ToString
public class InterceptContext {

    private final ApiKeys apiKeys;
    private final DirectionType directionType;
    private final Integer brokerId;
    private final ClientRequest clientRequest;


    public InterceptContext(ApiKeys apiKeys, DirectionType directionType, ClientRequest request) {
        this.apiKeys = apiKeys;
        this.directionType = directionType;
        this.brokerId = request.getNodeId();
        this.clientRequest = request;
    }

    public List<ApiKeys> apiKeys() {
        // For simplicity. Client only need to configure for ALTER_CONFIGS.
        // INCREMENTAL_ALTER_CONFIGS and ALTER_CONFIGS have the same logic
        if (ApiKeys.INCREMENTAL_ALTER_CONFIGS.equals(apiKeys)) {
            return List.of(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS);
        }
        return List.of(apiKeys);
    }
}

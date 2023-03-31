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

import com.google.common.annotations.VisibleForTesting;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.RebuilderTools;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class ApiVersionReBuilder extends AbstractReBuilder<ApiVersionsRequest, ApiVersionsResponse> {

    // ref: org.apache.kafka.common.message.FetchResponseData#SCHEMA_12
    public static final short MAX_VERSION_OF_FETCH = (short) 12;
    private static final Pair<Short,Short> DEFAULT_VERSIONS = Pair.of((short)0, Short.MAX_VALUE);
    private final Map<Short,Pair<Short,Short>> supportedKeys;

    public ApiVersionReBuilder(RebuilderTools rebuilderTools) {
        super(ApiKeys.API_VERSIONS, rebuilderTools);
        supportedKeys = java.util.Arrays.stream(ApiKeys.values()).collect(Collectors.toMap(e -> e.id, e-> Pair.of(e.oldestVersion(),e.latestVersion())));
    }

    @VisibleForTesting
    public ApiVersionReBuilder(RebuilderTools rebuilderTools, Map<Short,Pair<Short,Short>> supportedKeys) {
        super(ApiKeys.API_VERSIONS, rebuilderTools);
        this.supportedKeys = supportedKeys;
    }

    @Override
    public CompletionStage<ApiVersionsResponse> rebuildResponse(ApiVersionsResponse response, ClientRequest clientRequest) {
        // filter unsupported keys
        var versionCollection = new ApiVersionsResponseData.ApiVersionCollection();
        response.data().apiKeys().stream()
                .filter(apiVersion -> supportedKeys.containsKey(apiVersion.apiKey()))
                .peek(apiVersion -> {
                    apiVersion.setPrev(-2);
                    apiVersion.setNext(-2);
                    apiVersion.setMaxVersion(
                            supportedKeys.getOrDefault(apiVersion.apiKey(), DEFAULT_VERSIONS).getRight() < apiVersion.maxVersion()? supportedKeys.get(apiVersion.apiKey()).getRight() : apiVersion.maxVersion());
                    apiVersion.setMinVersion(
                            supportedKeys.getOrDefault(apiVersion.apiKey(), DEFAULT_VERSIONS).getLeft() > apiVersion.minVersion()? supportedKeys.get(apiVersion.apiKey()).getLeft() : apiVersion.minVersion());
                })
                .forEach(versionCollection::add);

        response.data().setApiKeys(versionCollection);
        //Set max version of fetch to 12, force client to use topic name.
        if (response.apiVersion(ApiKeys.FETCH.id).maxVersion() > MAX_VERSION_OF_FETCH) {
            response.apiVersion(ApiKeys.FETCH.id).setMaxVersion(MAX_VERSION_OF_FETCH);
        }
        return CompletableFuture.completedFuture(response);
    }

}

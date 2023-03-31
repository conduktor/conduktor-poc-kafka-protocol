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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ApiVersionReBuilderTest extends ComponentBaseTest {

    public static final short UNSUPPORTED_KEY = (short) 10008;

    @Test
    public void testRebuildApiVersionResponse_shouldLimitServerVersionToMatchClient() throws ExecutionException, InterruptedException {
        var lowerVersions = Arrays.stream(ApiKeys.values())
                .map(apiKey -> Pair.of(apiKey.id,
                        Pair.of(apiKey.id == ApiKeys.OFFSET_COMMIT.id ? 4 : apiKey.oldestVersion(),
                                apiKey.id == ApiKeys.PRODUCE.id ? 1 : apiKey.latestVersion())))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        var reBuilder = new ApiVersionReBuilder(mockRebuilderTools, lowerVersions);
        var response = initApiVersionResponse();
        var rebuilt = reBuilder.rebuildResponse(response, null).toCompletableFuture().get();
        // assert produce has been minimised
        assertThat(rebuilt.apiVersion(ApiKeys.PRODUCE.id).maxVersion())
                .isEqualTo((short) 1);
        // assert that Fetch has max version is MAX_VERSION_OF_FETCH
        assertThat(rebuilt.apiVersion(ApiKeys.FETCH.id).maxVersion())
                .isEqualTo(ApiVersionReBuilder.MAX_VERSION_OF_FETCH);
        var exceptionsApis = List.of( ApiKeys.PRODUCE.id,  ApiKeys.OFFSET_COMMIT.id,  ApiKeys.FETCH.id);
        // assert the rest
        response.data().apiKeys().stream()
                .filter(apiKey -> !CollectionUtils.containsAny(exceptionsApis, apiKey.apiKey()))
                .forEach(apiKey -> {
                    assertThat(apiKey.maxVersion())
                            .isEqualTo(ApiKeys.forId(apiKey.apiKey()).latestVersion());
                    assertThat(apiKey.minVersion())
                            .isEqualTo(ApiKeys.forId(apiKey.apiKey()).oldestVersion());
                });
    }

    @Test
    public void testRebuildApiVersionResponse_shouldRebuildMaxVersionOfFetchApi() throws ExecutionException, InterruptedException {
        var max = (short) 1000;
        var min = (short) 13;
        var reBuilder = new ApiVersionReBuilder(mockRebuilderTools);
        var response = initApiVersionResponse(min, max);
        var rebuilt = reBuilder.rebuildResponse(response, null).toCompletableFuture().get();
        // assert request
        assertThat(rebuilt.apiVersion(ApiKeys.FETCH.id).maxVersion())
                .isEqualTo(ApiVersionReBuilder.MAX_VERSION_OF_FETCH);
    }

    @Test
    public void testRebuildApiVersionResponse_shouldKeepOriginMinAndMaxVersionOfApisExceptFetch() throws ExecutionException, InterruptedException {
        var reBuilder = new ApiVersionReBuilder(mockRebuilderTools);
        var response = initApiVersionResponse();
        var rebuilt = reBuilder.rebuildResponse(response, null).toCompletableFuture().get();
        // assert request
        for (var apiKey : ApiKeys.values()) {
            if (apiKey == ApiKeys.FETCH) {
                continue;
            }
            assertThat(rebuilt.apiVersion(apiKey.id).maxVersion())
                    .isEqualTo(ApiKeys.forId(apiKey.id).latestVersion());
            assertThat(rebuilt.apiVersion(apiKey.id).minVersion())
                    .isEqualTo(ApiKeys.forId(apiKey.id).oldestVersion());
        }

    }

    @Test
    public void testRebuildApiVersionResponse_shouldStripUnsuportedApiVersions() throws ExecutionException, InterruptedException {
        var max = (short) 1000;
        var min = (short) 13;
        var reBuilder = new ApiVersionReBuilder(mockRebuilderTools);
        var response = initApiVersionResponse(min, max);
        var rebuilt = reBuilder.rebuildResponse(response, null).toCompletableFuture().get();

        assertThat(rebuilt.data().apiKeys().stream().anyMatch(apiVersion -> apiVersion.apiKey() == UNSUPPORTED_KEY))
                .isFalse();

    }


    protected ApiVersionsResponse initApiVersionResponse(short minVersion, short maxVersion) {
        var responseData = new ApiVersionsResponseData();
        var apiVersions = new ArrayList<ApiVersionsResponseData.ApiVersion>();
        for (var apiKey : ApiKeys.values()) {
            var version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(apiKey.id);
            version.setMinVersion(minVersion);
            version.setMaxVersion(maxVersion);
            apiVersions.add(version);
        }

        // bad ApiVersion (for cluster linking etc)
        var unsupportedVersion = new ApiVersionsResponseData.ApiVersion();
        unsupportedVersion.setApiKey(UNSUPPORTED_KEY);
        unsupportedVersion.setMinVersion(minVersion);
        unsupportedVersion.setMaxVersion(maxVersion);
        apiVersions.add(unsupportedVersion);

        responseData.setApiKeys(new ApiVersionsResponseData.ApiVersionCollection(apiVersions.iterator()));
        return new ApiVersionsResponse(responseData);
    }

    protected ApiVersionsResponse initApiVersionResponse() {
        var responseData = new ApiVersionsResponseData();
        var apiVersions = new ArrayList<ApiVersionsResponseData.ApiVersion>();
        for (var apiKey : ApiKeys.values()) {
            var version = new ApiVersionsResponseData.ApiVersion();
            version.setApiKey(apiKey.id);
            version.setMinVersion(apiKey.oldestVersion());
            version.setMaxVersion(apiKey.latestVersion());
            apiVersions.add(version);
        }

        responseData.setApiKeys(new ApiVersionsResponseData.ApiVersionCollection(apiVersions.iterator()));
        return new ApiVersionsResponse(responseData);
    }


}
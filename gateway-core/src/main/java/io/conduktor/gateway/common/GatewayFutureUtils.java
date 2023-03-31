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

package io.conduktor.gateway.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class GatewayFutureUtils {

    public static CompletionStage<Void> NULL_STAGE = CompletableFuture.completedStage(null);

    public static <T> List<T> gatherResults(List<CompletionStage<T>> futureResults) {
        var results = new ArrayList<T>(futureResults.size());
        futureResults.forEach(f -> results.add(f.toCompletableFuture().getNow(null)));
        return results;
    }

}

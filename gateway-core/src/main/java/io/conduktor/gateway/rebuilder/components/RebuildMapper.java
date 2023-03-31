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

import com.google.inject.Inject;
import io.conduktor.gateway.rebuilder.ReBuilder;
import io.conduktor.gateway.service.RebuilderTools;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class RebuildMapper {
    @Getter
    @Accessors(fluent = true)
    private final Map<ApiKeys, ReBuilder> rebuildMapper;
    private final ReBuilder defaultRebuilder;

    @Inject
    public RebuildMapper(RebuilderTools rebuilderTools) {
        this.rebuildMapper = new HashMap<>();
        addMapper(new DefaultMetadataReBuilder(rebuilderTools));
        addMapper(new DefaultFindCoordinatorReBuilder(rebuilderTools));
        addMapper(new DescribeClusterRebuilder(rebuilderTools));
        addMapper(new ApiVersionReBuilder(rebuilderTools));
        this.defaultRebuilder = new DefaultReBuilder(rebuilderTools);
    }

    public ReBuilder getReBuilder(ApiKeys key) {

        var rebuilder = rebuildMapper.get(key);
        if (Objects.isNull(rebuilder)) {
            log.debug("Not rebuilding: {} ", key);
            return defaultRebuilder;
        }
        return rebuilder;
    }

    private void addMapper(ReBuilder reBuilder) {
        rebuildMapper.put(reBuilder.apiKeys(), reBuilder);
    }

}

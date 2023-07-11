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

package io.conduktor.gateway.interceptor;

import org.apache.kafka.common.requests.AbstractRequestResponse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Plugin {

    List<InterceptorProvider<?>> getInterceptors(Map<String, Object> config) throws InterceptorConfigurationException;

    default String pluginId() {
        return this.getClass().getCanonicalName();
    }

    default Map<Class<?>, List<Interceptor<? extends AbstractRequestResponse>>> getTypedInterceptors(Map<String, Object> config) throws InterceptorConfigurationException {
        var result = new HashMap<Class<?>, List<Interceptor<?>>>();
        getInterceptors(config)
                .forEach(interceptor -> {
                    result.putIfAbsent(interceptor.type(), new ArrayList<>());
                    result.get(interceptor.type()).add(interceptor.interceptor());
                });
        return result;
    }

    default String readme() {
        try {
            return resourceAsString("/META-INF/services/README.md");
        } catch (Exception e) {
            return "";
        }
    }

    private String resourceAsString(String name) throws IOException, URISyntaxException {
        return Files.readString(
                Paths.get(getClass().getResource(name).toURI()),
                StandardCharsets.UTF_8);
    }

    default List<String> examples() {
        try {
            return List.of(resourceAsString("/META-INF/services/example.json"));
        } catch (Exception e) {
            return List.of();
        }
    }

    default Map<String, String> tags() {
        return new MarkdownHeadersUtil().extractHeaders(readme());
    }
}

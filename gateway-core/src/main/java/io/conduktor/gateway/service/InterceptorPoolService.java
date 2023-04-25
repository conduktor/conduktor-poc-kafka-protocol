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

package io.conduktor.gateway.service;

import io.conduktor.gateway.config.InterceptorConfigEntry;
import io.conduktor.gateway.config.InterceptorPluginConfig;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.interceptor.Interceptor;
import io.conduktor.gateway.interceptor.InterceptorConfigurationException;
import io.conduktor.gateway.interceptor.InterceptorValue;
import io.conduktor.gateway.interceptor.Plugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.AbstractResponse;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("rawtypes")
@Slf4j
public class InterceptorPoolService {

    private final GatewayConfiguration gatewayConfiguration;
    private final PluginLoader pluginLoader;

    private final Map<Class<?>, List<InterceptorValue>> interceptors = new HashMap<>();

    @Inject
    public InterceptorPoolService(GatewayConfiguration gatewayConfiguration, PluginLoader pluginLoader) {
        this.gatewayConfiguration = gatewayConfiguration;
        this.pluginLoader = pluginLoader;
        try {
            loadInterceptors();
        } catch (Exception e) {
            log.error("Failed to load interceptors", e);
            throw new RuntimeException(e);
        }
    }

    sealed interface PluginStatus {
        record Missing(InterceptorPluginConfig config) implements PluginStatus {
        }

        record Available(Plugin plugin) implements PluginStatus {
        }

        record Loaded(Plugin plugin, InterceptorPluginConfig config) implements PluginStatus {
        }
    }

    List<PluginStatus> loadPlugins(List<Plugin> plugins, List<InterceptorPluginConfig> configurations) {
        var configuredPluginIds = configurations.stream()
                .map(InterceptorPluginConfig::getPluginClass)
                .toList();
        return Stream.concat(
                configurations.stream()
                        .map(configuration -> plugins.stream().filter(plugin -> plugin.pluginId().equals(configuration.getPluginClass())).findFirst()
                                .map(plugin -> (PluginStatus) new PluginStatus.Loaded(plugin, configuration))
                                .orElseGet(() -> new PluginStatus.Missing(configuration))),
                plugins.stream()
                        .filter(plugin -> !configuredPluginIds.contains(plugin.pluginId()))
                        .map(PluginStatus.Available::new)
        ).toList();
    }

    @SuppressWarnings("unchecked")
    private void loadInterceptors() throws InterceptorConfigurationException {
        validateInterceptorPluginConfigs(gatewayConfiguration.getInterceptors());
        var pluginStatus = loadPlugins(pluginLoader.load(), gatewayConfiguration.getInterceptors());
        var loadedInterceptors = pluginStatus.stream()
                .peek(status -> {
                    if (status instanceof PluginStatus.Available available) {
                        log.debug("Plugin {} available but not configured", available.plugin().pluginId());
                    } else if (status instanceof PluginStatus.Missing missing) {
                        log.warn("Plugin {} configured but no matching plugin found", missing.config().getPluginClass());
                    }
                })
                .filter(status -> status instanceof PluginStatus.Loaded)
                .map(status -> (PluginStatus.Loaded) status)
                .toList();
        for (PluginStatus.Loaded loaded : loadedInterceptors) {
            log.debug("Loaded plugin {} with configuration", loaded.plugin.pluginId());
            var checkedConfig = loaded.config().getConfig().stream()
                    .peek(configValue -> {
                        if (Objects.isNull(configValue.getKey())) {
                            throw new IllegalArgumentException("key for config " + loaded.config().getPluginClass() + " can not be null");
                        }
                        if (Objects.isNull(configValue.getValue())) {
                            throw new IllegalArgumentException("value for config " + loaded.config().getPluginClass() + " of key " + configValue.getKey() + " can not be null");
                        }
                    })
                    .collect(Collectors.toMap(InterceptorConfigEntry::getKey, InterceptorConfigEntry::getValue));
            loaded.plugin.getTypedInterceptors(checkedConfig).forEach((type, interceptorsForType) -> {
                interceptors.putIfAbsent(type, new ArrayList<>());
                interceptors.get(type).addAll(interceptorsForType.stream()
                        .map(interceptor -> new InterceptorValue((Interceptor<AbstractRequestResponse>) interceptor, loaded.config().getPriority(), loaded.config().getTimeoutMs()))
                        .toList());
            });
        }
    }

    public List<InterceptorValue> getAllInterceptors(Class<?> inputType) {
        var filteredInterceptors = new ArrayList<InterceptorValue>();
        // add specific ones
        if (!List.of(AbstractRequestResponse.class,
                AbstractRequest.class,
                AbstractResponse.class).contains(inputType)
                && interceptors.containsKey(inputType)) {
            filteredInterceptors.addAll(interceptors.get(inputType));
        }
        // add directional ones
        if (interceptors.containsKey(AbstractRequest.class) && AbstractRequest.class.isAssignableFrom(inputType)) {
            filteredInterceptors.addAll(interceptors.get(AbstractRequest.class));
        }
        if (interceptors.containsKey(AbstractResponse.class) && AbstractResponse.class.isAssignableFrom(inputType)) {
            filteredInterceptors.addAll(interceptors.get(AbstractResponse.class));
        }

        // add everything ones
        if (interceptors.containsKey(AbstractRequestResponse.class)) {
            filteredInterceptors.addAll(interceptors.get(AbstractRequestResponse.class));
        }
        return filteredInterceptors;
    }

    private void validateInterceptorPluginConfigs(List<InterceptorPluginConfig> interceptors) {
        var isDuplicated = interceptors.stream()
                .collect(Collectors.groupingBy(InterceptorPluginConfig::getName))
                .entrySet()
                .stream()
                .anyMatch(e -> e.getValue().size() > 1);
        if (isDuplicated) {
            throw new IllegalArgumentException("Interceptor plugin config already exists");
        }
    }

}

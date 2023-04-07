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

package io.conduktor.gateway;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.conduktor.gateway.common.DefaultGatewayCacheProvider;
import io.conduktor.gateway.common.GatewayCacheProvider;
import io.conduktor.gateway.config.*;
import io.conduktor.gateway.config.kafka.*;
import io.conduktor.gateway.error.handler.ErrorHandler;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.model.BuildInfo;
import io.conduktor.gateway.network.BrokerManager;
import io.conduktor.gateway.network.BrokerManagerWithHostMapping;
import io.conduktor.gateway.network.BrokerManagerWithPortMapping;
import io.conduktor.gateway.network.GatewayBrokers;
import io.conduktor.gateway.service.*;
import io.conduktor.gateway.thread.UpStreamResource;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import javax.inject.Named;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;


@Slf4j
public class DependencyInjector extends AbstractModule {

    private final GatewayConfiguration gatewayConfiguration;

    public DependencyInjector(GatewayConfiguration gatewayConfiguration) throws IOException {
        this.gatewayConfiguration = gatewayConfiguration;
    }

    @Provides
    @Singleton
    public Time time() {
        return new SystemTime();
    }

    @Provides
    @Singleton
    public PrometheusMeterRegistry prometheusRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Provides
    @Singleton
    public MetricsRegistryProvider metricsRegistry(PrometheusMeterRegistry prometheusRegistry) {
        var loggingRegistry = new LoggingMeterRegistry();
        var compositeRegistry = new CompositeMeterRegistry();
        compositeRegistry.add(prometheusRegistry);
        compositeRegistry.add(loggingRegistry);
        // Gauges loaded and unloaded classes.
        new ClassLoaderMetrics().bindTo(compositeRegistry);
        // Gauges buffer and memory pool utilization.
        new JvmMemoryMetrics().bindTo(compositeRegistry);
        // Gauges max and live data size, promotion and allocation rates, and times GC pauses (or concurrent phase time in the case of CMS).
        new JvmGcMetrics().bindTo(compositeRegistry);
        // Gauges current CPU total and load average.
        new ProcessorMetrics().bindTo(compositeRegistry);
        // Gauges thread peak, number of daemon threads, and live threads.
        new JvmThreadMetrics().bindTo(compositeRegistry);
        new FileDescriptorMetrics().bindTo(compositeRegistry);
        return new MetricsRegistryProvider(compositeRegistry);
    }

    @Provides
    @Singleton
    BuildInfo buildInfo() {
        try (var buildPropertiesFile = this.getClass().getClassLoader().getResourceAsStream("build.properties")) {
            var properties = new Properties();
            properties.load(buildPropertiesFile);
            return Optional.ofNullable(properties.getProperty("build.version"))
                    .flatMap(version -> Optional.ofNullable(properties.getProperty("build.time"))
                            .flatMap(time -> Optional.ofNullable(properties.getProperty("build.commit"))
                                    .map(commit -> new BuildInfo(version, time, commit))))
                    .orElseThrow(() -> new IllegalStateException("Build info file doesn't have all required properties"));
        } catch (IOException | IllegalStateException e) {
            log.warn("Couldn't load build info properties", e);
            return null;
        }
    }

    @Provides
    @Singleton
    public KafkaPropertiesProvider kafkaPropertiesProvider(GatewayConfiguration gatewayConfiguration) {
        var selectorConfig = gatewayConfiguration.getKafkaSelector();
        // Switch of types is not by default in Java17 :disappointed:
        if (selectorConfig instanceof KafkaSelectorConfig.FileConfig fileConfig) {
            return new EnvVarTransformProvider(new FileKafkaPropertiesProvider(fileConfig.path()));
        } else if (selectorConfig instanceof KafkaSelectorConfig.EnvVarConfig envVarConfig) {
            return new EnvVarTransformProvider(new EnvKafkaPropertiesProvider(envVarConfig.prefix()));
        } else {
            throw new IllegalArgumentException("Kafka selector can only be File, EnvVar or Conduktor provider");
        }
    }

    @Provides
    @Singleton
    @Named("kafkaServerProperties")
    Properties kafkaServerProperties(KafkaPropertiesProvider kafkaProvider) throws IOException {
        return kafkaProvider.loadKafkaProperties();
    }


    @Provides
    @Singleton
    @Named("kafkaNodes")
    List<Node> kafkaNodes(ClientService clientService) throws IOException {
        return clientService.getKafkaNodes();
    }

    @Override
    protected void configure() {
        bind(GatewayConfiguration.class).toInstance(gatewayConfiguration);
        bind(UpstreamThreadConfig.class).annotatedWith(Names.named("upstreamThreadConfig"))
                .toInstance(gatewayConfiguration.getThreadConfig().getUpstream());
        bind(Integer.class).annotatedWith(Names.named("downstreamThread"))
                .toInstance(gatewayConfiguration.getThreadConfig().getDownStreamThread());
        bind(SslConfig.class).annotatedWith(Names.named("gatewaySslConfig"))
                .toInstance(gatewayConfiguration.getAuthenticationConfig().getSslConfig());
        bind(HostPortConfiguration.class).toInstance(gatewayConfiguration.getHostPortConfiguration());
        bind(ConnectionConfig.class).annotatedWith(Names.named("upStreamConnectionConfig"))
                .toInstance(gatewayConfiguration.getUpstreamConnectionConfig());
        bind(AuthenticationConfig.class).toInstance(gatewayConfiguration.getAuthenticationConfig());
        bind(UpStreamResource.class).in(Singleton.class);
        bind(GatewayBrokers.class).in(Singleton.class);
        bind(GatewayExecutor.class).in(Singleton.class);
        if (StringUtils.equals(gatewayConfiguration.getRouting(), "host")) {
            bind(BrokerManager.class).to(BrokerManagerWithHostMapping.class).in(Singleton.class);
        } else {
            bind(BrokerManager.class).to(BrokerManagerWithPortMapping.class).in(Singleton.class);
        }
        bind(InFlightRequestService.class).in(Singleton.class);
        bind(RebuilderTools.class).in(Singleton.class);
        bind(InterceptorPoolService.class).in(Singleton.class);
        bind(InterceptorOrchestration.class).in(Singleton.class);
        bind(ErrorHandler.class).in(Singleton.class);
        bind(Long.class).annotatedWith(Names.named("inFlightRequestExpiryMs"))
                .toInstance(gatewayConfiguration.getInFlightRequestExpiryMs());
        bind(ClientService.class).in(Singleton.class);
        bind(GatewayCacheProvider.class).to(DefaultGatewayCacheProvider.class).in(Singleton.class);

    }

}

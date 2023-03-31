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

package io.conduktor.gateway.network;

import com.google.common.collect.Lists;
import io.conduktor.gateway.authorization.BasicUserPoolSaslAuthentication;
import io.conduktor.gateway.authorization.NoneSecurityHandler;
import io.conduktor.gateway.authorization.SaslSecurityHandler;
import io.conduktor.gateway.authorization.SecurityHandler;
import io.conduktor.gateway.config.AuthenticationConfig;
import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.Endpoint;
import io.conduktor.gateway.config.HostPortConfiguration;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.service.ClientService;
import io.conduktor.gateway.thread.GatewayThread;
import io.conduktor.gateway.thread.UpStreamResource;
import io.conduktor.gateway.tls.FileSystemKeyStoreLoader;
import io.conduktor.gateway.tls.Filesystem;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;


@Slf4j
public abstract class BrokerManager implements AutoCloseable {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    protected final MetricsRegistryProvider metricsRegistryProvider;
    protected final String gatewayHost;
    protected final String kafkaBootstrapServer;
    protected final GatewayBrokers gatewayBrokers;
    private final ConcurrentHashMap<GatewayChannel, Integer> channels = new ConcurrentHashMap<>();
    protected AuthenticationConfig authenticationConfig;
    protected UpStreamResource upStreamResource;
    protected Supplier<GatewayChannelInitializer> channelInitializerSupplier;
    protected Node firstNode;
    private Map<String, SslContext> expandedKeystore;

    public BrokerManager(
            Properties properties,
            AuthenticationConfig authenticationConfig,
            HostPortConfiguration hostPortConfiguration,
            MetricsRegistryProvider metricsRegistryProvider,
            GatewayBrokers gatewayBrokers,
            ClientService clientService) {
        this.authenticationConfig = authenticationConfig;
        this.gatewayHost = hostPortConfiguration.getGatewayHost();
        this.metricsRegistryProvider = metricsRegistryProvider;
        this.gatewayBrokers = gatewayBrokers;
        if (authenticationConfig.getAuthenticatorType() == AuthenticatorType.SSL) {
            var sslConfig = authenticationConfig.getSslConfig();
            gatewayBrokers.getBossGroup().scheduleAtFixedRate(this::reloadSsl, 0, sslConfig.getUpdateContextIntervalMinutes(), TimeUnit.MINUTES);
            loadExpandedKeyStore();
        }
        this.channelInitializerSupplier = nettyChannelInitializerSupplier();
        this.kafkaBootstrapServer = properties.getProperty(BOOTSTRAP_SERVERS);
        this.firstNode = clientService.getAvailableKafkaNode(properties);
    }

    public void setUpstreamResourceAndStartBroker(UpStreamResource upStreamResource) {
        this.upStreamResource = upStreamResource;
        startBroker();
    }

    @Override
    public void close() throws Exception {
        for (GatewayChannel channel : channels.keySet()) {
            channel.close();
        }
    }

    public CompletableFuture<Void> reloadSsl() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                loadExpandedKeyStore();
            } catch (Exception exception) {
                log.error("error happen when load expanded ssl keystore", exception);
            }
            return null;
        });
    }

    public static Set<String> parseHostNames(X509Certificate cert) throws CertificateEncodingException {
        Set<String> hostNameList = new HashSet<>();
        X500Name x500name = new JcaX509CertificateHolder(cert).getSubject();
        String cn = x500name.getRDNs(BCStyle.CN)[0].getFirst().getValue().toString();
        hostNameList.add(cn);
        try {
            Optional.ofNullable(cert.getSubjectAlternativeNames())
                    .orElse(Lists.newArrayList())
                    .stream()
                    .filter(it -> it.size() > 1)
                    .forEach(altName -> {
                        switch ((Integer) altName.get(0)) {
                            case GeneralName.dNSName, GeneralName.iPAddress -> {
                                Object data = altName.get(1);
                                if (data instanceof String) {
                                    hostNameList.add(((String) data));
                                }
                            }
                            default -> {
                            }
                        }
                    });
        } catch (CertificateParsingException e) {
            log.error("Can't parse hostNames from this cert.", e);
        }
        return hostNameList;
    }


    public void loadExpandedKeyStore() {
        log.info("Loading keystore");
        var sslConfig = authenticationConfig.getSslConfig();
        try {
            var keyStoreProvider = new FileSystemKeyStoreLoader(Filesystem.DEFAULT_FILESYSTEM, sslConfig.getKeyStore());
            var loaded = keyStoreProvider.load();
            var keystore = loaded.getKeyStore();
            var expandedKeystore = new HashMap<String, SslContext>();
            keystore.aliases().asIterator().forEachRemaining(alias ->
                    {
                        try {
                            var key = (PrivateKey) keystore.getKey(alias, loaded.getKeyPassword());
                            var cert = (X509Certificate) keystore.getCertificate(alias);

                            var chain = Arrays.stream(keystore.getCertificateChain(alias))
                                    .map(x -> (X509Certificate) x)
                                    .toList();
                            var domains = parseHostNames(cert);
                            log.info("Detected domains for {} alias : {}", alias, String.join(", ", domains));
                            var sslContext = SslContextBuilder.forServer(key, chain).build();
                            domains.forEach(domain -> {
                                expandedKeystore.put(domain, sslContext);
                                log.info("Creating ssl context for {} domain", domain);
                            });
                        } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException |
                                 IOException | CertificateEncodingException e) {
                            throw new RuntimeException("Error loading " + alias + " alias from initial keystore to expanded one", e);
                        }
                    }
            );
            this.expandedKeystore = expandedKeystore;
        } catch (CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Error while expanding keystore", e);
        }
    }

    public Map<String, Endpoint> getRealToGatewayMap(MetadataResponseData.MetadataResponseBrokerCollection brokers) {
        return getRealToGatewayMap(
                brokers.stream().map(broker -> new Node(broker.nodeId(), broker.host(), broker.port())).toList()
        );
    }

    public Map<String, Endpoint> getRealToGatewayMap(DescribeClusterResponseData.DescribeClusterBrokerCollection brokers) {
        return getRealToGatewayMap(
                brokers.stream().map(broker -> new Node(broker.brokerId(), broker.host(), broker.port())).toList()
        );
    }

    public abstract Map<String, Endpoint> getRealToGatewayMap(List<Node> brokers);

    public abstract Endpoint getGatewayByReal(String host, int port);

    public abstract Node getRealNodeByGateway(SocketChannel socketChannel);


    protected abstract void startBroker();

    private Supplier<GatewayChannelInitializer> nettyChannelInitializerSupplier() {
        switch (authenticationConfig.getAuthenticatorType()) {
            case NONE, SASL_PLAINTEXT -> {
                return () -> new PlainServerChannelInitializer(metricsRegistryProvider, logicHandler());
            }
            case SSL, SASL_SSL -> {
                return () -> new SecureServerChannelInitializer(() -> expandedKeystore, gatewayHost, metricsRegistryProvider, logicHandler());
            }
        }
        return null;
    }

    @SuppressWarnings("resource")
    private Consumer<SocketChannel> logicHandler() {
        return gatewaySocketChannel -> {
            var gatewayThread = (GatewayThread) upStreamResource.next();
            SecurityHandler authenticator = switch (authenticationConfig.getAuthenticatorType()) {
                case NONE, SSL -> new NoneSecurityHandler();
                case SASL_PLAINTEXT, SASL_SSL -> new SaslSecurityHandler(gatewayThread, new BasicUserPoolSaslAuthentication(authenticationConfig.getUserPool()), gatewaySocketChannel, metricsRegistryProvider);
            };
            var gatewayChannel = new GatewayChannel(authenticator, this, gatewaySocketChannel, gatewayThread, gatewayHost);
            authenticator.setGatewayChannel(gatewayChannel);
            channels.put(gatewayChannel, 1);
            metricsRegistryProvider.trackGatewayChannel(gatewayChannel);
            gatewaySocketChannel.pipeline().addLast(gatewayChannel);
            gatewayChannel.getGatewaySocketChannel().closeFuture().addListener(future -> {
                gatewayChannel.close();
                channels.remove(gatewayChannel);
            });
        };
    }


}

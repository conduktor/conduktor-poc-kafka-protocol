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

package io.conduktor.gateway.integration.tls;

import io.conduktor.gateway.config.AuthenticatorType;
import io.conduktor.gateway.config.GatewayConfiguration;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import static javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class GatewayTLSTest extends BaseGatewayIntegrationTest {


    @Test
    void testReloadCertificates() throws Exception {
        //keystore:   test/config/old/kafka-gateway.keystore.jks
        //truststore: test/config/old/kafka-gateway.truststore.jks
        try (var s = createClientSocket("config/tls/old/kafka-gateway.truststore.jks")) {

            var session = ((SSLSocket) s).getSession();
            var cchain = session.getPeerCertificates();

            assertThat(cchain).hasSize(1);
            var cert = (X509Certificate) cchain[0];
            assertThat(cert.getNotAfter()).hasYear(2023);

            //reload keystore, replace keystore by new keystore with new certificate
            useKeyStore("config/tls/new/new-kafka-gateway.keystore.jks");
        }

        //keystore:   test/config/new/new-kafka-gateway.keystore.jks
        //truststore: test/config/new/new-kafka-gateway.truststore.jks
        try (var s = createClientSocket("config/tls/new/new-kafka-gateway.truststore.jks")){

            var session = ((SSLSocket) s).getSession();
            var cchain = session.getPeerCertificates();

            assertThat(cchain).hasSize(1);
            var cert = (X509Certificate) cchain[0];
            assertThat(cert.getNotAfter()).hasYear(2022);
        }
    }

    @Override
    protected void reconfigureGateway(GatewayConfiguration gatewayConfiguration) {
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyStorePath("../gateway-test/config/tls/kafka-gateway.keystore.jks");
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyStorePassword("123456");
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyPassword("123456");
        gatewayConfiguration.getAuthenticationConfig().getSslConfig().getKeyStore().setKeyStoreType("jks");
    }

    @Override
    protected AuthenticatorType getAuthenticatorType() {
        return AuthenticatorType.SSL;
    }

    @BeforeAll
    protected void beforeAll() throws Exception {
        useKeyStore("config/tls/old/kafka-gateway.keystore.jks");
    }

    @AfterAll
    protected void afterAll() throws Exception {
        useKeyStore("config/tls/old/kafka-gateway.keystore.jks");
    }

    private Socket createClientSocket(String truststorePath) throws KeyStoreException, IOException {
        var keyStore = loadTrustStore(truststorePath, "123456", "jks");
        var sslContext = createSslContextForClient(keyStore);
        var ssf = sslContext.getSocketFactory();
        return ssf.createSocket("localhost", getGatewayPort());
    }

    private void useKeyStore(String newKeyStorePath) throws Exception {
        updateKeyStoreFile(newKeyStorePath);
        getBrokerManager().reloadSsl().get();
    }

    private void updateKeyStoreFile(String keystorePath) throws IOException {
        var keystore = new File("config/tls/kafka-gateway.keystore.jks");
        var newKeyStore = new File(keystorePath);
        FileUtils.copyFile(newKeyStore, keystore);
    }

    private KeyStore loadTrustStore(String path, String password, String type)
            throws KeyStoreException {
        final char[] keyStorePassword = password.toCharArray();
        final KeyStore keyStore = KeyStore.getInstance(type);
        try (final InputStream is = new FileInputStream(path)) {
            keyStore.load(is, keyStorePassword);
        } catch (Throwable throwable) {
            log.error("Error happen when load keystore", throwable);
            throw new RuntimeException(throwable);
        }
        return keyStore;
    }

    private SSLContext createSslContextForClient(KeyStore keyStore) {
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            try {
                trustManagerFactory.init(keyStore);
            } catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
            final TrustManager[] trustManagers = createTrustManagers(keyStore);
            sc.init(null, trustManagers, new SecureRandom());
            return sc;
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to initialize upstream SSL handler", e);
        } catch (KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    private X509TrustManager[] createTrustManagers(final KeyStore keyStore) {
        try {
            final TrustManagerFactory factory = TrustManagerFactory.getInstance(getDefaultAlgorithm());
            factory.init(keyStore);
            return Arrays.stream(factory.getTrustManagers())
                    .filter(it -> it instanceof X509TrustManager)
                    .map(it -> (X509TrustManager) it)
                    .toArray(X509TrustManager[]::new);
        } catch (final KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to setup trust manager", e);
        }
    }

}

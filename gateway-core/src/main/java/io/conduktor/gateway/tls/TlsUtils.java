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

package io.conduktor.gateway.tls;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class TlsUtils {

    public static KeyManagerFactory createKeyManagerFactory(final KeyStoreProvider keyStoreProvider) {
        try {
            final KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            if (Objects.nonNull(keyStoreProvider)) {
                final KeyStoreWrapper keyStoreWrapper = keyStoreProvider.getKeyStore();
                factory.init(keyStoreWrapper.getKeyStore(), keyStoreWrapper.getKeyPassword());
            } else {
                factory.init(null, null);
            }
            return factory;
        } catch (final UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to setup key manager", e);
        }

    }


    public static SslContext createSslContextNetty(
            final KeyStoreConfig keyStoreConfig) {
        try {
            var keyStoreProvider = new ReloadingKeyStoreProvider(
                    new FileSystemKeyStoreLoader(Filesystem.DEFAULT_FILESYSTEM, keyStoreConfig),
                    System::currentTimeMillis,
                    keyStoreConfig.getUpdateIntervalMsecs()
            );
            return SslContextBuilder.forServer(createKeyManagerFactory(keyStoreProvider)).build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    public static CompletableFuture<SslContext> createSslContextNettyAsync(
            final KeyStoreConfig keyStoreConfig) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var keyStoreProvider = new ReloadingKeyStoreProvider(
                        new FileSystemKeyStoreLoader(Filesystem.DEFAULT_FILESYSTEM, keyStoreConfig),
                        System::currentTimeMillis,
                        keyStoreConfig.getUpdateIntervalMsecs()
                );
                return SslContextBuilder.forServer(createKeyManagerFactory(keyStoreProvider)).build();
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
        });

    }


}

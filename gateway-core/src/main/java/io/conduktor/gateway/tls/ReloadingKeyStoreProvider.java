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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
public class ReloadingKeyStoreProvider implements KeyStoreProvider {

    private final KeyStoreLoader loader;
    private final Supplier<Long> clock;
    private final long updateIntervalMsecs;
    private final Object keyStoreLock = new Object();
    private final Object clockLock = new Object();
    private final AtomicBoolean loading = new AtomicBoolean();
    private long lastUpdate;
    private KeyStoreWrapper keyStore;

    public ReloadingKeyStoreProvider(
            final KeyStoreLoader loader,
            final Supplier<Long> clock,
            final long updateIntervalMsecs
    ) {
        this.loader = loader;
        this.clock = clock;
        this.updateIntervalMsecs = updateIntervalMsecs;
    }

    public static Optional<ReloadingKeyStoreProvider> createReloader(
            final Optional<KeyStoreConfig> keystore,
            final Supplier<Long> clock,
            final Filesystem filesystem
    ) {
        return keystore.map(it -> createReloader(it, clock, filesystem));
    }

    public static ReloadingKeyStoreProvider createReloader(
            final KeyStoreConfig keystore,
            final Supplier<Long> clock,
            final Filesystem filesystem
    ) {
        return new ReloadingKeyStoreProvider(
                new FileSystemKeyStoreLoader(filesystem, keystore),
                clock,
                keystore.getUpdateIntervalMsecs()
        );
    }

    @Override
    public KeyStoreWrapper getKeyStore() {
        updateIfNecessary();
        synchronized (keyStoreLock) {
            return keyStore;
        }
    }

    private void updateIfNecessary() {
        if (!updateIsNecessary()) {
            return;
        }
        if (loading.getAndSet(true)) {
            // Update already in progress
            return;
        }
        try {
            final KeyStoreWrapper newKeyStore = loader.load();
            synchronized (keyStoreLock) {
                keyStore = newKeyStore;
            }
            synchronized (clockLock) {
                lastUpdate = clock.get();
            }
        } catch (final Exception e) {
            log.warn("Failed to reload keystore", e);
        } finally {
            loading.set(false);
        }
    }

    private boolean updateIsNecessary() {
        synchronized (clockLock) {
            final long now = clock.get();
            final long timePassedSinceLastUpdate = now - lastUpdate;
            return timePassedSinceLastUpdate > updateIntervalMsecs;
        }
    }

    public interface KeyStoreLoader {

        KeyStoreWrapper load() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException;

    }

}

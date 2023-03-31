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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class FileSystemKeyStoreLoader implements ReloadingKeyStoreProvider.KeyStoreLoader {

    private final Filesystem filesystem;
    private final KeyStoreConfig config;

    public FileSystemKeyStoreLoader(
            final Filesystem filesystem,
            final KeyStoreConfig config
    ) {
        this.filesystem = filesystem;
        this.config = config;
    }

    @Override
    public KeyStoreWrapper load()
            throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        final char[] keyStorePassword = config.getKeyStorePassword().toCharArray();
        final char[] keyPassword = config.getKeyPassword().toCharArray();
        final KeyStore keyStore = KeyStore.getInstance(config.getKeyStoreType());
        try (final InputStream is = new ByteArrayInputStream(filesystem.readFile(config.getKeyStorePath()))) {
            keyStore.load(is, keyStorePassword);
        }
        return new KeyStoreWrapper(keyStore, keyPassword);
    }


}
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

import java.security.KeyStore;

public class KeyStoreWrapper {
    private final KeyStore keyStore;
    private final char[] keyPassword;

    public KeyStoreWrapper(final KeyStore keyStore, final char[] keyPassword) {
        this.keyStore = keyStore;
        this.keyPassword = keyPassword;
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }


    public char[] getKeyPassword() {
        return keyPassword;
    }
}

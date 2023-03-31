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

package io.conduktor.gateway.authorization;

import io.conduktor.gateway.config.AuthenticationConfig;
import io.conduktor.gateway.model.User;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class BasicUserPoolSaslAuthentication implements SaslAuthentication {

    private final List<AuthenticationConfig.UserCredential> credentials;

    @Override
    public Optional<User> authenticate(String username, String password) {
        return credentials.stream()
                .filter(credential -> credential.username().equals(username) && credential.password().equals(password))
                .findFirst()
                .map(credential -> new User(username));
    }
}

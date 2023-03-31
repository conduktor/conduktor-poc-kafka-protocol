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

import io.conduktor.gateway.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.plain.PlainAuthenticateCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.util.Optional;

@Slf4j
public class LoginCallbackHandler implements CallbackHandler {

    private final SaslAuthentication authenticator;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<User> user;

    public LoginCallbackHandler(SaslAuthentication authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        String username = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback)
                username = ((NameCallback) callback).getDefaultName();
            else if (callback instanceof PlainAuthenticateCallback plainCallback) {
                boolean authenticated = authenticate(username, plainCallback.password());
                plainCallback.authenticated(authenticated);
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }


    protected boolean authenticate(String username, char[] password) {
        if (username == null) {
            return false;
        }
        user = authenticator.authenticate(username, String.valueOf(password));
        return user.isPresent();
    }

    public Optional<User> getUser() {
        return user;
    }
}

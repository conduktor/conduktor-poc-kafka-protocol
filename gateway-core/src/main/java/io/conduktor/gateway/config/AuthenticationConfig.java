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

package io.conduktor.gateway.config;

import io.conduktor.gateway.service.validator.IgnoreWhen;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@IgnoreWhen(when = "this.authenticatorType != 'SSL' && this.authenticatorType != 'SASL_SSL' ", fields = {"sslConfig"})
@IgnoreWhen(when = "this.authenticatorType != 'SASL_PLAINTEXT' && this.authenticatorType != 'SASL_SSL'", fields = {"userPool"})
public class AuthenticationConfig {

    @NotNull
    private AuthenticatorType authenticatorType = AuthenticatorType.NONE;
    @Valid
    @NotNull
    private SslConfig sslConfig;
    @NotEmpty
    private List<UserCredential> userPool = Collections.emptyList();

    public AuthenticationConfig(AuthenticatorType authenticatorType, SslConfig sslConfig) {
        this.authenticatorType = authenticatorType;
        this.sslConfig = sslConfig;
    }

    public void setAuthenticatorType(AuthenticatorType authenticatorType) {
        if (Objects.isNull(authenticatorType)) {
            return;
        }
        this.authenticatorType = authenticatorType;
    }

    public record UserCredential(String username, String password) {
    }

}

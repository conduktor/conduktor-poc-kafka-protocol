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
import io.conduktor.gateway.network.GatewayChannel;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface SecurityHandler extends Closeable {


    void authenticate(ByteBuf byteBuf) throws Exception;

    boolean complete();

    Optional<User> getUser();

    void setGatewayChannel(GatewayChannel channel);

    CompletableFuture<Void> handleAuthenticationFailure();


}

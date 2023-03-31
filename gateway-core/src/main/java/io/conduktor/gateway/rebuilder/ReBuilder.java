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

package io.conduktor.gateway.rebuilder;

import io.conduktor.gateway.service.ClientRequest;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

public interface ReBuilder {

    ApiKeys apiKeys();


    /**
     * rebuild request, payload just contains readable bytes of request body. Request header already parsed before.
     * rebuilder may contain business logic with heavy IO, so we use CompletionStage to not affect event loop thread of Netty
     *
     * @param kafkaPayload  contains readable bytes of request body. Request header already parsed before.
     * @param clientRequest a request
     * @return rebuilt payload in {@link ByteBuffer} ready to sent to kafka cluster.
     */
    CompletionStage<ByteBuffer> rebuildRequest(ByteBuffer kafkaPayload, ClientRequest clientRequest);

    /**
     * rebuild response from kafka server to send back to client
     * rebuilder may contain business logic with heavy IO, so we use CompletionStage to not affect event loop thread of Netty
     *
     * @param payload       response from kafka
     * @param clientRequest request of this response
     * @return rebuild payload in {@link  ByteBuf} ready to send back to client
     */
    CompletionStage<ByteBuf> rebuildResponse(ByteBuf payload, ClientRequest clientRequest);

}

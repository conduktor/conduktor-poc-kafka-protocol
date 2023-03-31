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

package io.conduktor.gateway.service;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteBuffer;

import static io.conduktor.gateway.common.Constants.SIZE_BYTES;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.apache.kafka.common.requests.RequestUtils.serialize;

public class SerdeService {

    public static ByteBuf toResponse(AbstractResponse response, ClientRequest clientRequest) {
        var clientRequestHeader = clientRequest.getClientRequestHeader();
        var responseHeaderVersion = clientRequestHeader.apiKey().responseHeaderVersion(clientRequestHeader.apiVersion());
        var responseHeader = new ResponseHeader(clientRequest.getGatewayCorrelationId(), responseHeaderVersion);
        return wrappedBuffer(serialize(
                responseHeader.data(),
                responseHeader.headerVersion(),
                response.data(),
                clientRequestHeader.apiVersion()
        ));
    }

    public static AbstractRequest fromBuffer(ApiKeys apiKey, short apiVersion, ByteBuffer buffer) {
        return AbstractRequest.parseRequest(apiKey, apiVersion, buffer)
                .request;
    }

    public static ByteBuf toBuffer(ClientRequest clientRequest, AbstractResponse response) {
        var requestHeader = clientRequest.getClientRequestHeader();
        var responseHeader = new ResponseHeader(clientRequest.getClientCorrelationId(), requestHeader.apiKey().responseHeaderVersion(requestHeader.apiVersion()));
        var buf = wrappedBuffer(serialize(
                responseHeader.data(),
                responseHeader.headerVersion(),
                response.data(),
                requestHeader.apiVersion()
        ));
        var responseHeaderByteBuf = buffer(SIZE_BYTES);
        responseHeaderByteBuf.writeInt(buf.readableBytes());
        var wrappedBuffer = wrappedBuffer(responseHeaderByteBuf, buf);
        wrappedBuffer.resetReaderIndex();
        return wrappedBuffer;
    }

    public static AbstractResponse fromBuffer(ByteBuffer buf, RequestHeader requestHeader) {
        return AbstractResponse.parseResponse(buf, requestHeader);
    }

}

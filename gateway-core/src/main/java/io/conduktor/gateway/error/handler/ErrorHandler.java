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

package io.conduktor.gateway.error.handler;

import com.google.inject.Inject;
import io.conduktor.gateway.common.ExceptionUtils;
import io.conduktor.gateway.error.handler.components.DefaultResponseErrorBuilder;
import io.conduktor.gateway.rebuilder.exception.GatewayIntentionException;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.RebuilderTools;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteBuffer;

import static io.conduktor.gateway.thread.GatewayThread.RESPONSE_BUFFER_INITIAL_CAPACITY;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.apache.kafka.common.requests.RequestUtils.serialize;

public class ErrorHandler {

    private final RebuilderTools rebuilderTools;
    @SuppressWarnings("rawtypes")
    private final ResponseErrorBuilder responseErrorBuilder;

    @SuppressWarnings("rawtypes")
    @Inject
    public ErrorHandler(RebuilderTools rebuilderTools) {
        this.rebuilderTools = rebuilderTools;
        this.responseErrorBuilder = new DefaultResponseErrorBuilder();
    }

    @SuppressWarnings({"unchecked", "AccessStaticViaInstance"})
    public void handleRequestError(ClientRequest clientRequest, ByteBuffer kafkaPayload) {
        var requestHeader = clientRequest.getClientRequestHeader();
        var request = rebuilderTools.serdeService().fromBuffer(requestHeader.apiKey(), requestHeader.apiVersion(), kafkaPayload);
        var errorResponse = responseErrorBuilder.fromRequest(request);
        var bufferResponse = rebuilderTools.serdeService().toBuffer(clientRequest, errorResponse);
        clientRequest.marKDoneWithResponse(bufferResponse);
    }

    @SuppressWarnings({"unchecked", "AccessStaticViaInstance"})
    public void handleResponseError(ClientRequest clientRequest, Throwable throwable) {
        throwable = ExceptionUtils.getRootCause(throwable);
        var gatewayRequestHeader = clientRequest.getGatewayRequestHeader();
        //use gateway request header to deserialize response from server
        var request = rebuilderTools.serdeService().fromBuffer(gatewayRequestHeader.apiKey(), gatewayRequestHeader.apiVersion(), clientRequest.getRequest());
        var errorResponse = responseErrorBuilder.fromRequest(request, throwable);
        var bufferResponse = rebuilderTools.serdeService().toBuffer(clientRequest, errorResponse);
        clientRequest.marKDoneWithResponse(bufferResponse);
    }

    public boolean handleGatewayException(ClientRequest clientRequest, Throwable cause) {
        if (cause instanceof GatewayIntentionException gatewayIntentionException) {
            handleInterceptorIntentionError(clientRequest, gatewayIntentionException);
            return true;
        }
        return false;
    }

    private void handleInterceptorIntentionError(ClientRequest clientRequest, GatewayIntentionException intentionError) {
        var response = intentionError.getErrorResponse();
        var buf = getResponseBuf(clientRequest, response);
        var responseHeaderByteBuf = buffer(RESPONSE_BUFFER_INITIAL_CAPACITY);
        responseHeaderByteBuf.writeInt(buf.readableBytes());
        clientRequest.marKDoneWithResponse(wrappedBuffer(responseHeaderByteBuf, buf));
    }

    private ByteBuf getResponseBuf(ClientRequest clientRequest, AbstractResponse response) {
        // clients ignore the provided header version and use one based on apiVersion
        short headerVersion = clientRequest.getClientRequestHeader().apiKey().responseHeaderVersion(clientRequest.getClientRequestHeader().apiVersion());
        var responseHeader = new ResponseHeader(clientRequest.getClientCorrelationId(), headerVersion);
        var requestHeader = clientRequest.getClientRequestHeader();

        var serialized = serialize(
                responseHeader.data(),
                headerVersion,
                response.data(),
                requestHeader.apiVersion()
        );
        return wrappedBuffer(serialized);
    }

}

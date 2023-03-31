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

package io.conduktor.gateway.rebuilder.components;

import io.conduktor.gateway.interceptor.DirectionType;
import io.conduktor.gateway.model.InterceptContext;
import io.conduktor.gateway.rebuilder.ReBuilder;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.InterceptorOrchestration;
import io.conduktor.gateway.service.RebuilderTools;
import io.conduktor.gateway.service.SerdeService;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static io.conduktor.gateway.common.Constants.SIZE_BYTES;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.apache.kafka.common.requests.RequestUtils.serialize;

@Slf4j
public abstract class AbstractReBuilder<REQUEST extends AbstractRequest, RESPONSE extends AbstractResponse> implements ReBuilder {

    protected final ApiKeys apiKeys;
    protected final RebuilderTools rebuilderTools;
    private final InterceptorOrchestration interceptorOrchestration;

    public AbstractReBuilder(ApiKeys apiKeys, RebuilderTools rebuilderTools) {
        this.apiKeys = apiKeys;
        this.rebuilderTools = rebuilderTools;
        this.interceptorOrchestration = rebuilderTools.interceptorOrchestration();
    }

    @Override
    public ApiKeys apiKeys() {
        return apiKeys;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletionStage<ByteBuffer> rebuildRequest(ByteBuffer kafkaPayload, ClientRequest clientRequest) {
        var clientRequestHeader = clientRequest.getClientRequestHeader();
        var gatewayRequestHeader = clientRequest.getGatewayRequestHeader();
        var request = SerdeService.fromBuffer(clientRequestHeader.apiKey(), clientRequestHeader.apiVersion(), kafkaPayload);
        log.debug("Received {} request from client id: {} with correlation id {}: {}",
                clientRequestHeader.apiKey().name,
                clientRequestHeader.clientId(),
                clientRequest.getClientCorrelationId(),
                request);
        var context = new InterceptContext(clientRequestHeader.apiKey(), DirectionType.REQUEST, clientRequest);
        return interceptorOrchestration.intercept(context, request)
                .thenCompose(interceptedRequest -> rebuildRequest((REQUEST) interceptedRequest, clientRequest))
                .thenCompose(rebuiltPayload -> {

                            log.debug("Rebuilt {} request id {}: {}", clientRequestHeader.apiKey().name, clientRequest.getClientCorrelationId(), rebuiltPayload);
                            //use gateway request header to serialize request to send to kafka
                            var convertedBuffer = serialize(
                                    gatewayRequestHeader.data(),
                                    gatewayRequestHeader.headerVersion(),
                                    rebuiltPayload.data(),
                                    gatewayRequestHeader.apiVersion()
                            );
                            var totalSize = SIZE_BYTES + convertedBuffer.remaining();
                            var responseByteBuffer = ByteBuffer.allocate(totalSize);
                            responseByteBuffer.putInt(convertedBuffer.remaining());
                            responseByteBuffer.put(convertedBuffer);
                            responseByteBuffer.rewind();
                            return CompletableFuture.completedFuture(responseByteBuffer);
                        });
    }


    @SuppressWarnings("unchecked")
    @Override
    public CompletionStage<ByteBuf> rebuildResponse(ByteBuf buf, ClientRequest clientRequest) {
        buf.resetReaderIndex();
        var clientRequestHeader = clientRequest.getClientRequestHeader();
        var gatewayRequestHeader = clientRequest.getGatewayRequestHeader();
        //use gateway request header to deserialize response from server
        var response = SerdeService.fromBuffer(buf.nioBuffer(), gatewayRequestHeader);
        log.debug("Received {} response id {}: {}", gatewayRequestHeader.apiKey().name, clientRequest.getClientCorrelationId(), response);
        CompletionStage<RESPONSE> result;
        result = rebuildResponse((RESPONSE) response, clientRequest);
        return result.handle((rebuiltResponse, ex) -> {
                    if (ex != null) {
                        log.error("Error happen when rebuild response", ex);
                        throw new CompletionException(ex);
                    } else {
                        log.debug("Rebuilt {} response id {}: {}", gatewayRequestHeader.apiKey().name, clientRequest.getClientCorrelationId(), rebuiltResponse);
                        return rebuiltResponse;
                    }
                })
                .thenCompose(rebuiltResponse -> {
                    var context = new InterceptContext(clientRequestHeader.apiKey(), DirectionType.RESPONSE, clientRequest);
                    return interceptorOrchestration.intercept(context, rebuiltResponse);
                })
                .thenApply(interceptedResponse -> {
                    var responseHeaderVersion = clientRequestHeader.apiKey().responseHeaderVersion(clientRequestHeader.apiVersion());
                    var responseHeader = ResponseHeader.parse(buf.nioBuffer(), responseHeaderVersion);
                    responseHeader.data().setCorrelationId(clientRequest.getClientCorrelationId());
                    return wrappedBuffer(serialize(
                            responseHeader.data(),
                            responseHeader.headerVersion(),
                            interceptedResponse.data(),
                            clientRequestHeader.apiVersion()
                    ));
                });
    }


    /**
     * Rebuilder component should override this method to rebuild request
     *
     * @param request       original request
     * @param clientRequest keep track of request
     * @return a {@link  CompletionStage} contains rebuilt request for caller to wait on
     */
    public CompletionStage<REQUEST> rebuildRequest(REQUEST request, ClientRequest clientRequest) {
        return CompletableFuture.completedFuture(request);
    }

    /**
     * Rebuilder component should override this method to rebuild response
     *
     * @param response      original response
     * @param clientRequest keep track of request
     * @return a {@link  CompletionStage} contains rebuilt response for caller to wait on
     */
    public CompletionStage<RESPONSE> rebuildResponse(RESPONSE response, ClientRequest clientRequest) {
        return CompletableFuture.completedFuture(response);
    }

}

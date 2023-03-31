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

import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.model.User;
import io.conduktor.gateway.network.GatewayChannel;
import io.conduktor.gateway.thread.GatewayThread;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.kerberos.KerberosError;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.utils.Utils;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.conduktor.gateway.common.Constants.SIZE_BYTES;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.requests.RequestUtils.serialize;

@Slf4j
public class SaslSecurityHandler implements SecurityHandler {

    private static final String PLAIN_MECHANISM = "PLAIN";
    private final LoginCallbackHandler authenticateCallbackHandler;
    private final GatewayThread gatewayThread;
    private final SocketChannel socketChannel;
    private boolean enableKafkaSaslAuthenticateHeaders;
    private GatewayChannel gatewayChannel;
    private SaslServer saslServer;


    private Supplier<CompletableFuture<Void>> authenticationFailureHandler;
    private final MetricsRegistryProvider metricsRegistryProvider;
    private String saslMechanism;
    // Next SASL state to be set when outgoing writes associated with the current SASL state complete
    // private GatewaySaslAuthenticator.SaslState pendingSaslState = null;
    // Exception that will be thrown by `authenticate()` when SaslState is set to FAILED after outbound writes complete
    // private AuthenticationException pendingException = null;
    // Current SASL state
    private SaslState saslState = SaslState.INITIAL_REQUEST;

    public SaslSecurityHandler(
            GatewayThread gatewayThread,
            SaslAuthentication saslAuthenticator,
            SocketChannel socketChannel,
            MetricsRegistryProvider metricsRegistryProvider) {
        this.authenticateCallbackHandler = new LoginCallbackHandler(saslAuthenticator);
        this.enableKafkaSaslAuthenticateHeaders = false;
        this.gatewayThread = gatewayThread;
        this.socketChannel = socketChannel;
        this.metricsRegistryProvider = metricsRegistryProvider;
    }

    @Override
    public void close() throws IOException {
        if (saslServer != null)
            saslServer.dispose();
    }

    @Override
    public void authenticate(ByteBuf byteBuf) throws Exception {
        try {
            switch (saslState) {
                case REAUTH_PROCESS_HANDSHAKE, HANDSHAKE_OR_VERSIONS_REQUEST, HANDSHAKE_REQUEST, INITIAL_REQUEST ->
                        handleKafkaRequest(byteBuf);
                case REAUTH_BAD_MECHANISM -> throw new SaslAuthenticationException("reauth bad mechanism");

                // For default GSSAPI, fall through to authenticate using the client token as the first GSSAPI packet.
                // This is required for interoperability with 0.9.0.x clients which do not send handshake request
                case AUTHENTICATE -> {
                    handleSaslToken(byteBuf);
                    // When the authentication exchange is complete and no more tokens are expected from the client,
                    // update SASL state. Current SASL state will be updated when outgoing writes to the client complete.
                    if (saslServer.isComplete()) {
                        setSaslState(SaslState.COMPLETE);
                    }
                }
                default -> {
                }
            }
        } catch (Exception e) {
            log.warn("Failed during authenticate", e);
            if (e instanceof AuthenticationException) {
                // Exception will be propagated after response is sent to client
                setSaslState(SaslState.FAILED, (AuthenticationException) e);
                throw e;
            } else {
                saslState = SaslState.FAILED;
            }
        }
    }

    @Override
    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    @Override
    public Optional<User> getUser() {
        if (saslServer == null || !saslServer.isComplete()) {
            return Optional.empty();
        }
        return authenticateCallbackHandler.getUser();
    }

    @Override
    public void setGatewayChannel(GatewayChannel channel) {
        this.gatewayChannel = channel;
    }

    public RequestAndSize parseRequest(RequestHeader requestHeader, ByteBuffer byteBuffer) {
        if (isUnsupportedApiVersionsRequest(requestHeader)) {
            // Unsupported ApiVersion requests are treated as v0 requests and are not parsed
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, requestHeader.apiVersion());
            return new RequestAndSize(apiVersionsRequest, 0);
        } else {
            ApiKeys apiKey = requestHeader.apiKey();
            try {
                short apiVersion = requestHeader.apiVersion();
                return AbstractRequest.parseRequest(apiKey, apiVersion, byteBuffer);
            } catch (Throwable ex) {
                throw new InvalidRequestException("Error getting request for apiKey: " + apiKey +
                        ", apiVersion: " + requestHeader.apiVersion() +
                        ", principal: " + KafkaPrincipal.ANONYMOUS, ex);
            }
        }
    }

    public boolean isUnsupportedApiVersionsRequest(RequestHeader requestHeader) {
        return requestHeader.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(requestHeader.apiVersion());
    }


    private void setSaslState(SaslState saslState) {
        setSaslState(saslState, null);
    }

    private void sendKafkaResponse(RequestHeader requestHeader, AbstractResponse response) {
        var payload = buildByteBufResponse(requestHeader, response);
        sendKafkaResponse(payload);
    }

    private void sendKafkaResponse(ByteBuf payload) {
        payload.resetReaderIndex();
        var responseHeaderByteBuf = buffer(SIZE_BYTES);
        responseHeaderByteBuf.writeInt(payload.readableBytes());
        socketChannel.writeAndFlush(wrappedBuffer(responseHeaderByteBuf, payload));
    }


    private CompletableFuture<Void> sendFailResponse(ByteBuf payload) {
        payload.resetReaderIndex();
        var responseHeaderByteBuf = buffer(SIZE_BYTES);
        responseHeaderByteBuf.writeInt(payload.readableBytes());
        var result = new CompletableFuture<Void>();
        socketChannel.writeAndFlush(wrappedBuffer(responseHeaderByteBuf, payload))
                .addListener(rs -> result.complete(null));
        return result;
    }

    private ByteBuf buildByteBufResponse(RequestHeader requestHeader, AbstractResponse response) {

        short headerVersion = requestHeader.apiKey().responseHeaderVersion(requestHeader.apiVersion());
        var responseHeader = new ResponseHeader(requestHeader.correlationId(), headerVersion);

        var serialized = serialize(
                responseHeader.data(),
                headerVersion,
                response.data(),
                requestHeader.apiVersion()
        );
        return wrappedBuffer(serialized);

    }

    private void handleKafkaRequest(ByteBuf byteBuf) {
        String clientMechanism = null;
        try {
            ByteBuffer requestBuffer = byteBuf.nioBuffer();
            var header = RequestHeader.parse(requestBuffer);
            ApiKeys apiKey = header.apiKey();
            // A valid Kafka request header was received. SASL authentication tokens are now expected only
            // following a SaslHandshakeRequest since this is not a GSSAPI client token from a Kafka 0.9.0.x client.
            if (saslState == SaslState.INITIAL_REQUEST) {
                setSaslState(SaslState.HANDSHAKE_OR_VERSIONS_REQUEST);
            }
            // Raise an error prior to parsing if the api cannot be handled at this layer. This avoids
            // unnecessary exposure to some of the more complex schema types.
            if (apiKey != ApiKeys.API_VERSIONS && apiKey != ApiKeys.SASL_HANDSHAKE)
                throw new IllegalSaslStateException("Unexpected Kafka request of type " + apiKey + " during SASL handshake.");

            log.debug("Handling Kafka request {}", apiKey);
            if (apiKey == ApiKeys.API_VERSIONS) {
                handleApiVersionsRequest(byteBuf);
            } else {
                RequestAndSize requestAndSize = parseRequest(header, requestBuffer);
                clientMechanism = handleHandshakeRequest(header, (SaslHandshakeRequest) requestAndSize.request);
            }
        } catch (InvalidRequestException e) {
            if (saslState == SaslState.INITIAL_REQUEST) {
                // InvalidRequestException is thrown if the request is not in Kafka format or if the API key
                // is invalid. For compatibility with 0.9.0.x where the first packet is a GSSAPI token
                // starting with 0x60, revert to GSSAPI for both these exceptions.
                if (log.isDebugEnabled()) {
                    StringBuilder tokenBuilder = new StringBuilder();
                    for (byte b : ByteBufUtil.getBytes(byteBuf)) {
                        tokenBuilder.append(String.format("%02x", b));
                        if (tokenBuilder.length() >= 20)
                            break;
                    }
                    log.debug("Received client packet of length {} starting with bytes 0x{}, process as GSSAPI packet", byteBuf.readableBytes(), tokenBuilder);
                }
                throw new UnsupportedSaslMechanismException("Exception handling first SASL packet from client, GSSAPI is not supported by server", e);
            } else
                throw e;
        }
        if (clientMechanism != null) {
            createSaslServer(clientMechanism);
            setSaslState(SaslState.AUTHENTICATE);
        }
    }

    private void handleSaslToken(ByteBuf byteBuf) throws IOException {
        if (!enableKafkaSaslAuthenticateHeaders) {
            byte[] response = saslServer.evaluateResponse(ByteBufUtil.getBytes(byteBuf));
            if (response != null) {
                sendKafkaResponse(byteBuf);
            }
        } else {
            ByteBuffer requestBuffer = byteBuf.nioBuffer();
            RequestHeader requestHeader = RequestHeader.parse(requestBuffer);
            ApiKeys apiKey = requestHeader.apiKey();
            short version = requestHeader.apiVersion();
            RequestAndSize requestAndSize = parseRequest(requestHeader, requestBuffer);
            if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
                IllegalSaslStateException e = new IllegalSaslStateException("Unexpected Kafka request of type " + apiKey + " during SASL authentication.");
                buildResponseOnAuthenticateFailure(requestHeader, requestAndSize.request.getErrorResponse(e));
                throw e;
            }
            if (!apiKey.isVersionSupported(version)) {
                // We cannot create an error response if the request version of SaslAuthenticate is not supported
                // This should not normally occur since clients typically check supported versions using ApiVersionsRequest
                throw new UnsupportedVersionException("Version " + version + " is not supported for apiKey " + apiKey);
            }
            /*
             * The client sends multiple SASL_AUTHENTICATE requests, and the client is known
             * to support the required version if any one of them indicates it supports that
             * version.
             */
            SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) requestAndSize.request;

            try {
                byte[] responseToken = saslServer.evaluateResponse(
                        Utils.copyArray(saslAuthenticateRequest.data().authBytes()));
                byte[] responseBytes = responseToken == null ? new byte[0] : responseToken;
                sendKafkaResponse(requestHeader, new SaslAuthenticateResponse(
                        new SaslAuthenticateResponseData()
                                .setErrorCode(Errors.NONE.code())
                                .setAuthBytes(responseBytes)));
            } catch (SaslAuthenticationException e) {
                buildResponseOnAuthenticateFailure(requestHeader,
                        new SaslAuthenticateResponse(
                                new SaslAuthenticateResponseData()
                                        .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                                        .setErrorMessage(e.getMessage())));
                throw e;
            } catch (SaslException e) {
                KerberosError kerberosError = KerberosError.fromException(e);
                if (kerberosError != null && kerberosError.retriable()) {
                    // Handle retriable Kerberos exceptions as I/O exceptions rather than authentication exceptions
                    throw e;
                } else {
                    // DO NOT include error message from the `SaslException` in the client response since it may
                    // contain sensitive data like the existence of the user.
                    String errorMessage = "Authentication failed due to invalid credentials with SASL mechanism " + saslMechanism;
                    buildResponseOnAuthenticateFailure(requestHeader, new SaslAuthenticateResponse(
                            new SaslAuthenticateResponseData()
                                    .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                                    .setErrorMessage(errorMessage)));
                    throw new SaslAuthenticationException(errorMessage, e);
                }
            }
        }
    }

    private void handleApiVersionsRequest(ByteBuf byteBuf) {
        if (saslState != SaslState.HANDSHAKE_OR_VERSIONS_REQUEST) {
            throw new IllegalStateException("Unexpected ApiVersions request received during SASL authentication state " + saslState);
        }
        setSaslState(SaslState.HANDSHAKE_REQUEST);
        gatewayThread.justSend(byteBuf, gatewayChannel);
    }

    private String handleHandshakeRequest(RequestHeader requestHeader, SaslHandshakeRequest handshakeRequest) throws UnsupportedSaslMechanismException {
        String clientMechanism = handshakeRequest.data().mechanism();
        short version = requestHeader.apiVersion();
        if (version >= 1)
            this.enableKafkaSaslAuthenticateHeaders(true);
        if (PLAIN_MECHANISM.equals(clientMechanism)) {
            log.debug("Using SASL mechanism '{}' provided by client", clientMechanism);
            sendKafkaResponse(requestHeader, new SaslHandshakeResponse(
                    new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()).setMechanisms(List.of(PLAIN_MECHANISM))));
            return clientMechanism;
        } else {
            log.debug("SASL mechanism '{}' requested by client is not supported", clientMechanism);
            buildResponseOnAuthenticateFailure(requestHeader, new SaslHandshakeResponse(
                    new SaslHandshakeResponseData().setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code()).setMechanisms(List.of(PLAIN_MECHANISM))));
            throw new UnsupportedSaslMechanismException("Unsupported SASL mechanism " + clientMechanism);
        }
    }

    private void setSaslState(SaslState saslState, AuthenticationException exception) {
        this.saslState = saslState;
        log.debug("Set SASL server state to {}", saslState);
        if (exception != null)
            throw exception;
    }

    private void enableKafkaSaslAuthenticateHeaders(boolean flag) {
        this.enableKafkaSaslAuthenticateHeaders = flag;
    }

    private void buildResponseOnAuthenticateFailure(RequestHeader requestHeader, AbstractResponse response) {
        var authenticationFailurePayload = buildByteBufResponse(requestHeader, response);
        sendFailResponse(authenticationFailurePayload);
    }


    @Override
    public CompletableFuture<Void> handleAuthenticationFailure() {
        if (Objects.nonNull(authenticationFailureHandler)) {
            return authenticationFailureHandler.get();
        }
        return CompletableFuture.completedFuture(null);
    }

    private void createSaslServer(String mechanism) {
        if (!mechanism.equals("PLAIN")) {
            throw new RuntimeException("we are just handling PLAIN mechanism not" + mechanism);
        }
        this.saslMechanism = mechanism;
        this.saslServer = new PlainSaslServer(authenticateCallbackHandler);
    }

    public enum SaslState {
        INITIAL_REQUEST,               // May be GSSAPI token, SaslHandshake or ApiVersions for authentication
        HANDSHAKE_OR_VERSIONS_REQUEST, // May be SaslHandshake or ApiVersions
        HANDSHAKE_REQUEST,             // After an ApiVersions request, next request must be SaslHandshake
        AUTHENTICATE,                  // Authentication tokens (SaslHandshake v1 and above indicate SaslAuthenticate headers)
        COMPLETE,                      // Authentication completed successfully
        FAILED,                        // Authentication failed
        REAUTH_PROCESS_HANDSHAKE,      // Initial state for re-authentication, processes SASL handshake request
        REAUTH_BAD_MECHANISM,          // When re-authentication requested with wrong mechanism, generate exception
    }

}

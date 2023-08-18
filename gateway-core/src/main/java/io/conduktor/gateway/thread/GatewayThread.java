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

package io.conduktor.gateway.thread;

import io.conduktor.gateway.config.ConnectionConfig;
import io.conduktor.gateway.error.handler.ErrorHandler;
import io.conduktor.gateway.metrics.MetricsRegistryKeys;
import io.conduktor.gateway.metrics.MetricsRegistryProvider;
import io.conduktor.gateway.network.GatewayChannel;
import io.conduktor.gateway.network.UpstreamIO;
import io.conduktor.gateway.rebuilder.components.RebuildMapper;
import io.conduktor.gateway.interceptor.InterceptorIntentionException;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.InFlightRequestService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Gateway thread which use {@link org.apache.kafka.common.network.Selector} to do the IO with kafka cluster behind
 * maintain a task queue to:
 * - rebuild request from client
 * - rebuild response from server
 * - keep order of request when sending to server and when sending response to client
 * It has the similar flow as {@link io.netty.channel.nio.NioEventLoop}
 * It extends from {@link  SingleThreadEventLoop} to reuse some methods, to work with {@link io.netty.channel.MultithreadEventLoopGroup}
 * It is managed by {@link UpStreamResource}
 * and to inherit more from Netty.
 */
@Slf4j
public class GatewayThread extends SingleThreadEventLoop {

    public static final int RESPONSE_BUFFER_INITIAL_CAPACITY = 4;
    private final InFlightRequestService inFlightRequestService;
    private final UpstreamIO upstreamIOOrchestration;
    private final RebuildMapper rebuildMapper;
    private final ErrorHandler errorHandler;
    private final MetricsRegistryProvider metricsRegistryProvider;
    private final Time time = Time.SYSTEM;
    private final Counter receivedRequestCounter;


    private volatile long gracefulShutdownQuietPeriodCustom;

    private volatile long startShutDownGraceFullyAt;
    private volatile long gracefulShutdownTimeoutCustom;


    public GatewayThread(
            EventLoopGroup parent,
            Executor executor,
            RejectedExecutionHandler rejectedExecutionHandler,
            RebuildMapper rebuildMapper,
            Properties selectorProps,
            ConnectionConfig connectionConfig,
            int maxPendingTask,
            InFlightRequestService inFlightRequestService,
            ErrorHandler errorHandler,
            MetricsRegistryProvider metricsRegistryProvider) {
        super(parent, executor, false, PlatformDependent.newMpscQueue(maxPendingTask), PlatformDependent.newMpscQueue(), rejectedExecutionHandler);
        this.upstreamIOOrchestration = new UpstreamIO(selectorProps, connectionConfig);
        this.inFlightRequestService = inFlightRequestService;
        this.rebuildMapper = rebuildMapper;
        this.errorHandler = errorHandler;
        this.metricsRegistryProvider = metricsRegistryProvider;
        var threadProperties = this.threadProperties();

        metricsRegistryProvider.registry().gauge(MetricsRegistryKeys.THREAD_TASKS, Tags.of("threadId",
                String.valueOf(threadProperties.id()), "name", threadProperties.name()), this, GatewayThread::pendingTasks);
        upstreamIOOrchestration.registerMetrics(metricsRegistryProvider.registry(), Tags.of("threadId", String.valueOf(threadProperties.id()), "name", threadProperties.name()));
        receivedRequestCounter = metricsRegistryProvider.registry().counter(MetricsRegistryKeys.THREAD_RECEIVED_REQUEST, Tags.of("threadId", String.valueOf(threadProperties.id()), "name", threadProperties.name()));
    }

    /**
     * Try to execute the given {@link Runnable} and just log if it throws a {@link Throwable}.
     */
    protected static void safeExecute(Runnable task) {
        if (Objects.isNull(task)) {
            return;
        }
        try {
            task.run();
        } catch (Throwable t) {
            log.warn("A task raised an exception. Task: {}", task, t);
        }
    }

    // copied from https://github.com/netty/netty/blob/278b49b2a791968c6b80ed0995ef25771b3fd654/transport/src/main/java/io/netty/channel/nio/NioEventLoop.java#L494
    private static void handleLoopException(Throwable t) {
        log.warn("Unexpected exception in the selector loop.", t);
        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    public void registerNode(Node node) {
        upstreamIOOrchestration.registerNode(node);
    }

    public void deregisterKafkaNode(Node node) {
        upstreamIOOrchestration.deregisterKafkaNode(node);
    }

    public void justSend(ByteBuf buf, GatewayChannel gatewayChannel) {
        //TODO: what will happen when the task queue is full?
        receivedRequestCounter.increment();
        buf.retain();
        execute(() -> rebuild(buf, gatewayChannel));
    }

    public void close() {
        this.shutdownGracefully();
    }

    public void closeAll() {
        try {
            upstreamIOOrchestration.close();
        } catch (IOException e) {
            log.error("error happen when close connection pool");
        }
    }

    /**
     * Poll all tasks from the task queue and run them via {@link Runnable#run()} method.  This method stops running
     * the tasks in the task queue and returns if it ran longer than {@code timeoutNanos}.
     */
    protected boolean runAllTasks(long timeoutNanos) {
        Runnable task = pollTask();

        final long deadline = timeoutNanos > 0 ? getCurrentTimeNanos() + timeoutNanos : 0;
        long runTasks = 0;
        long lastExecutionTime;
        for (; ; ) {
            safeExecute(task);

            runTasks++;
            // Check timeout every 64 tasks because nanoTime() is relatively expensive.
            // XXX: Hard-coded value - will make it configurable if it is really a problem.
            if ((runTasks & 0x3F) == 0) {
                lastExecutionTime = getCurrentTimeNanos();
                if (lastExecutionTime >= deadline) {
                    break;
                }
            }
            task = pollTask();
            if (task == null) {
                break;
            }
        }
        return true;
    }

    @Override
    public void run() {
        var threadProperties = this.threadProperties();
        this.upstreamIOOrchestration.registerMetrics(metricsRegistryProvider.registry(), Tags.of("threadId",
                String.valueOf(threadProperties.id()), "name", threadProperties.name()));
        for (; ; ) { // not a busy spin as we are polling and selector will wait on IO or wake up (new task or timeout)
            try {
                runAllTasks(Long.MAX_VALUE);
                if (isShuttingDown()) {
                    //should not wait longer than time left for shutting down the gateway thread
                    var timeFromShutDown = System.currentTimeMillis() - startShutDownGraceFullyAt;
                    var timeLeftForWaitingForShutdown = gracefulShutdownQuietPeriodCustom - timeFromShutDown;
                    upstreamIOOrchestration.poll(timeLeftForWaitingForShutdown);
                } else {
                    upstreamIOOrchestration.poll(Long.MAX_VALUE);
                }

                handleDisconnections();
                handleReceivers();
            } catch (IOException e) {
                // If we receive an IOException here its because the Selector is messed up. Let's rebuild
                // the selector and retry. https://github.com/netty/netty/issues/8566
                //TODO: somehow, we can rebuild the selector
//                rebuildSelector0();
                handleLoopException(e);
            } finally {
                // Always handle shutdown even if the loop processing threw an exception.
                try {
                    if (isShuttingDown()) {
                        closeAll();
                        if (confirmShutdown()) {
                            return;
                        }
                    }
                } catch (Error e) {
                    throw e;
                } catch (Throwable t) {
                    handleLoopException(t);
                }
            }
        }
    }


    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        gracefulShutdownQuietPeriodCustom = unit.toMillis(quietPeriod);
        startShutDownGraceFullyAt = System.currentTimeMillis();
        gracefulShutdownTimeoutCustom = unit.toMillis(timeout);
        return super.shutdownGracefully(quietPeriod, timeout, unit);
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop) {
            upstreamIOOrchestration.wakeup();
        }
    }

    private void scheduleQueueRequestToKafka(ClientRequest clientRequest) {
        execute(() -> queueRequestToSendToKafka(clientRequest));
    }

    private void scheduleSendResponse(ClientRequest clientRequest) {
        execute(() -> clientRequest.getGatewayChannel().sendResponse());
    }


    private void rebuild(ByteBuf buf, GatewayChannel gatewayChannel) {
        var kafkaPayload = buf.nioBuffer();
        var upStreamConnection = upstreamIOOrchestration.getAssociatedConnection(gatewayChannel);
        var connectionId = upStreamConnection.getConnectionId();
        var requestHeader = RequestHeader.parse(kafkaPayload);
        var clientRequest = ClientRequest.initRequest(gatewayChannel, requestHeader, kafkaPayload.duplicate(),
                connectionId, inFlightRequestService, this::scheduleSendResponse, this::scheduleQueueRequestToKafka
        );
        var threadProperties = this.threadProperties();
        metricsRegistryProvider.registry().counter(MetricsRegistryKeys.THREAD_REBUILD_REQUEST, Tags.of("threadId",
                String.valueOf(threadProperties.id()), "name", threadProperties.name(), "api_key", requestHeader.apiKey().name())).increment();
        log.trace("Send request of apiKey {}, correlationId {}", clientRequest.getClientRequestHeader().apiKey(), clientRequest.getClientCorrelationId());
        //use new generated correlation id to send to kafka server
        gatewayChannel.enqueueRequest(clientRequest);
        try {
            rebuildMapper.getReBuilder(requestHeader.apiKey())
                    .rebuildRequest(kafkaPayload.duplicate(), clientRequest)
                    .thenAccept(rebuiltPayload -> {
                        if (clientRequest.isReadyToReceiver()) {
                            markRequestDone(Unpooled.wrappedBuffer(rebuiltPayload), clientRequest);
                        } else {
                            var byteBufferSend = new ByteBufferSend(rebuiltPayload);
                            var networkSend = new NetworkSend(clientRequest.getConnectionId(), byteBufferSend);
                            clientRequest.readyToSendToKafka(networkSend);
                        }
                    })
                    .exceptionally(ex -> {
                        inFlightRequestService.getAndRemoveRequest(clientRequest.getClientCorrelationId());
                        var cause = ex.getCause();
                        logErrorIfRequired(String.format("An error occurred when sending request to Kafka cluster: %s", requestHeader),
                                ex);
                        if (errorHandler.handleGatewayException(clientRequest, cause)) {
                            return null;
                        }
                        errorHandler.handleRequestError(clientRequest, kafkaPayload.duplicate());
                        return null;
                    });
        } catch (Exception exception) {
            logErrorIfRequired(String.format("An exception was thrown when during request send to Kafka cluster at: %s", requestHeader),
                    exception);
            errorHandler.handleRequestError(clientRequest, kafkaPayload.duplicate());
        } finally {
            buf.release();
        }
    }

    private void logErrorIfRequired(String message, Throwable exception) {
        if (exception instanceof InterceptorIntentionException && !((InterceptorIntentionException) exception).isLogAtErrorLevel()) {
            return;
        }
        log.error(message, exception);
    }

    private void queueRequestToSendToKafka(ClientRequest clientRequest) {
        if (clientRequest.getGatewayChannel().isClosed()) {
            return;
        }
        var topRequest = clientRequest.getGatewayChannel().nextRequestToSend();
        if (Objects.nonNull(topRequest) && topRequest.getGatewayCorrelationId() == clientRequest.getGatewayCorrelationId()) {
            if (upstreamIOOrchestration.trySend(topRequest)) {
                clientRequest.getGatewayChannel().pollTopToSend();
                return;
            }
        }
        scheduleQueueRequestToKafka(clientRequest);
    }

    /**
     * when our connection got terminated by the server.
     * what should we do?
     */
    private void handleDisconnections() {
        for (var connectionId : upstreamIOOrchestration.disconnected()) {
            processDisconnection(connectionId);
        }
    }

    private void processDisconnection(String connectionId) {
        upstreamIOOrchestration.disconnect(connectionId);
    }

    private void handleReceivers() {
        for (var receive : upstreamIOOrchestration.completedReceives()) {
            handleReceiver(receive);
        }
    }

    private void handleReceiver(NetworkReceive receive) {
        var buf = Unpooled.wrappedBuffer(receive.payload());
        var clientRequest = retrieveClientRequest(receive.source(), buf);
        try {
            if (clientRequest == null) return;
            var requestHeader = clientRequest.getClientRequestHeader();
            log.trace("receiver response of request {}, correlationId {} for source {}", clientRequest.getClientRequestHeader().apiKey(), clientRequest.getClientCorrelationId(), receive.source());
            var reBuilder = rebuildMapper.getReBuilder(requestHeader.apiKey());
            reBuilder.rebuildResponse(buf, clientRequest)
                    .thenAccept(rebuiltBuf -> markRequestDone(rebuiltBuf, clientRequest))
                    .exceptionally(ex -> {
                        log.error("Error happen when send response to client: {}", requestHeader, ex);
                        errorHandler.handleResponseError(clientRequest, ex);
                        return null;
                    });
        } catch (Exception ex) {
            log.error("Cannot rebuild message of request {}, correlationId {}", clientRequest.getClientRequestHeader().apiKey(), clientRequest.getClientCorrelationId(), ex);
            errorHandler.handleResponseError(clientRequest, ex);
        }
    }

    private void markRequestDone(ByteBuf buf, ClientRequest clientRequest) {
        var responseHeaderByteBuf = buffer(RESPONSE_BUFFER_INITIAL_CAPACITY);
        responseHeaderByteBuf.writeInt(buf.readableBytes());
        log.trace("Done rebuild, mark done  of request {}, correlationId {}", clientRequest.getClientRequestHeader().apiKey(), clientRequest.getClientCorrelationId());
        clientRequest.marKDoneWithResponse(wrappedBuffer(responseHeaderByteBuf, buf));
        recordExecutionTime(clientRequest);
    }

    private void recordExecutionTime(ClientRequest clientRequest) {
        if (Objects.isNull(!clientRequest.isRecordingMetrics())) {
            return;
        }
        var timeDelta = time.milliseconds() - clientRequest.getSendToKafkaStartTime();
        metricsRegistryProvider.globalTimer()
                .record(timeDelta, TimeUnit.MILLISECONDS);
    }

    private ClientRequest retrieveClientRequest(String source, ByteBuf buf) {
        int correlationId = -1;
        try {
            correlationId = buf.readInt();
            var clientRequest = inFlightRequestService.getAndRemoveRequest(correlationId);
            if (Objects.isNull(clientRequest)) {
                log.warn("not found request of correlationId {}", correlationId);
                return null;
            }
            return clientRequest;
        } catch (Exception ex) {
            log.error("Error happen when retrieve client request, source {}, correlationId {}", source, correlationId, ex);
            return null;
        }
    }

}

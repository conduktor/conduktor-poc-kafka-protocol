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

package io.conduktor.gateway.integration.error.handler;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.conduktor.gateway.integration.BaseGatewayIntegrationTest;
import io.conduktor.gateway.integration.util.ClientFactory;
import io.conduktor.gateway.rebuilder.components.RebuildMapper;
import io.conduktor.gateway.service.ClientRequest;
import io.conduktor.gateway.service.RebuilderTools;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ApiKeys;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class BaseErrorHandlerTest extends BaseGatewayIntegrationTest {

    public static final String KEY_1 = "key1";
    public static final String VAL_1 = "val1";

    protected String generateClientTopic() throws ExecutionException, InterruptedException {
        var clientTopicName = createTopic(clientFactory.kafkaAdmin(), 1, (short) 1);
        return clientTopicName;
    }

    protected void mockException(ExceptionMockType type, ApiKeys apiKeys) {
        var rebuildMapper = getRebuildMapper();
        var reBuilder = Mockito.spy(rebuildMapper.getReBuilder(apiKeys));
        when(rebuildMapper.getReBuilder(apiKeys))
                .thenAnswer(invocationOnMock -> reBuilder);
        if (type.equals(ExceptionMockType.REQUEST)) {
            doThrow(new RuntimeException("mock exception"))
                    .when(reBuilder).rebuildRequest(any(ByteBuffer.class), any(ClientRequest.class));
        } else {
            doThrow(new RuntimeException("mock exception"))
                    .when(reBuilder).rebuildResponse(any(ByteBuf.class), any(ClientRequest.class));
        }
    }

    protected AbstractModule mockGuiceModule() {
        return new MockGuideModule();
    }

    @Override
    protected void reconfigureClientFactory(ClientFactory clientFactory) {
        super.reconfigureClientFactory(clientFactory);
        clientFactory.addGatewayPropertyOverride("max.block.ms", "3000");
        clientFactory.addGatewayPropertyOverride("delivery.timeout.ms", "3000");
        clientFactory.addGatewayPropertyOverride("request.timeout.ms", "3000");
        clientFactory.addGatewayPropertyOverride("session.timeout.ms", "3000");
        clientFactory.addGatewayPropertyOverride("heartbeat.interval.ms", "2000");
        clientFactory.addGatewayPropertyOverride("retries", "2");
    }

    protected enum ExceptionMockType {
        REQUEST,
        RESPONSE
    }

    static class MockGuideModule extends AbstractModule {

        @Provides
        @Singleton
        public RebuildMapper rebuildMapper(RebuilderTools rebuilderTools) {
            var instance = new RebuildMapper(rebuilderTools);
            return Mockito.spy(instance);
        }

    }

}

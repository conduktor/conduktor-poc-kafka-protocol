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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InFlightRequestService {

    private final AtomicInteger correlationIdCounter;
    private final Cache<Integer, ClientRequest> requestMap;

    private final long inFlightRequestExpiryMs;

    @Inject
    public InFlightRequestService(@Named("inFlightRequestExpiryMs") long inFlightRequestExpiryMs) {
        this.correlationIdCounter = new AtomicInteger(0);
        this.inFlightRequestExpiryMs = inFlightRequestExpiryMs;
        this.requestMap = Caffeine.newBuilder()
                .expireAfter(expireInflightRequestAfter())
                .build();

    }

    public int trackRequest(ClientRequest clientRequest) {
        var newCorrelationId = correlationIdCounter.incrementAndGet();
        requestMap.put(newCorrelationId, clientRequest);
        return newCorrelationId;
    }

    public ClientRequest getAndRemoveRequest(int correlationId) {
        var request = requestMap.getIfPresent(correlationId);
        requestMap.invalidate(correlationId);
        return request;
    }

    private Expiry<Integer, ClientRequest> expireInflightRequestAfter() {
        return new Expiry<>() {
            @Override
            public long expireAfterCreate(Integer key, ClientRequest clientRequest, long currentTime) {
                return TimeUnit.MILLISECONDS.toNanos(inFlightRequestExpiryMs);
            }

            @Override
            public long expireAfterUpdate(Integer key, ClientRequest clientRequest, long currentTime, @NonNegative long currentDuration) {
                return currentDuration;
            }

            @Override
            public long expireAfterRead(Integer key, ClientRequest clientRequest, long currentTime, @NonNegative long currentDuration) {
                return currentDuration;
            }
        };
    }
}

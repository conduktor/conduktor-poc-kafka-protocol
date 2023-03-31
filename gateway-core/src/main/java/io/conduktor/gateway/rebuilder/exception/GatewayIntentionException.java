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

package io.conduktor.gateway.rebuilder.exception;

import lombok.Getter;
import org.apache.kafka.common.requests.AbstractResponse;

/**
 * An error with intention for quickly reply to client
 */
public class GatewayIntentionException extends RuntimeException{

    @Getter
    private final AbstractResponse errorResponse;

    @Getter
    private final boolean logAtErrorLevel;

    public GatewayIntentionException(AbstractResponse errorResponse) {

        this.errorResponse = errorResponse;
        this.logAtErrorLevel = true;
    }

    public GatewayIntentionException(String message, AbstractResponse errorResponse) {
        super(message);
        this.errorResponse = errorResponse;
        this.logAtErrorLevel = true;
    }

    public GatewayIntentionException(AbstractResponse errorResponse, boolean logAtErrorLevel) {
        this.errorResponse = errorResponse;
        this.logAtErrorLevel = logAtErrorLevel;
    }
}

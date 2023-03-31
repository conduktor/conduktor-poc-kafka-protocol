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

package io.conduktor.gateway.error.handler.components;

import io.conduktor.gateway.error.handler.ResponseErrorBuilder;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.List;
import java.util.Objects;

import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.protocol.Errors.maybeUnwrapException;

public class DefaultResponseErrorBuilder<I extends AbstractRequest, O extends AbstractResponse> implements ResponseErrorBuilder<I, O> {

    private static final List<Class<?>> handleableErrors = List.of(InvalidTopicException.class, PolicyViolationException.class);

    @SuppressWarnings("unchecked")
    @Override
    public O fromRequest(I request, Throwable throwable) {
        return (O) request.getErrorResponse(forException(throwable));
    }

    private Throwable forException(Throwable t) {
        if (Objects.isNull(t)) {
            return UNKNOWN_SERVER_ERROR.exception();
        }
        var cause = maybeUnwrapException(t);
        var clazz = cause.getClass();
        if (handleableErrors.contains(clazz)) {
            return t;
        }
        return UNKNOWN_SERVER_ERROR.exception();
    }


}

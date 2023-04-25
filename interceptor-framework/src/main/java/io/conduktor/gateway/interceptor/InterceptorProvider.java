package io.conduktor.gateway.interceptor;

import org.apache.kafka.common.requests.AbstractRequestResponse;

public record InterceptorProvider<T extends AbstractRequestResponse>(Class<? extends T> type,
                                                                     Interceptor<T> interceptor) {

}

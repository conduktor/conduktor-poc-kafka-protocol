package io.conduktor.gateway.interceptor;

public class InterceptorConfigurationException extends Exception {

    public InterceptorConfigurationException(String message) {
        super(message);
    }

    public InterceptorConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}

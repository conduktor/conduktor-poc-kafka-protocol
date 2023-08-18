package io.conduktor.gateway.exception;


import lombok.Getter;

/**
 * Use to return a meaningful reason why proxy fails on starting to user
 */

public class GatewayStartFailException extends RuntimeException{

    @Getter
    private final String reason;

    public GatewayStartFailException(String reason) {
        this.reason = reason;
    }
}

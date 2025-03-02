package com.jonathanfoucher.kafkastream.errors;

public class HttpServerCreationException extends RuntimeException {
    public HttpServerCreationException(Exception exception) {
        super("Error while creating HTTP server", exception);
    }
}

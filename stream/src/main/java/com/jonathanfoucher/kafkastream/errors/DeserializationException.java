package com.jonathanfoucher.kafkastream.errors;

public class DeserializationException extends RuntimeException {
    public DeserializationException(String str, Exception exception) {
        super("JSON deserialization error: " + str, exception);
    }
}

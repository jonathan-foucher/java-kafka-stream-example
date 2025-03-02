package com.jonathanfoucher.kafkastream.errors;

import com.jonathanfoucher.kafkastream.data.dto.MovieJsonKey;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonValue;

public class StreamProcessingException extends RuntimeException {
    public StreamProcessingException(MovieJsonKey key, MovieJsonValue value, Throwable error) {
        super("Error processing stream key: " + key + " value: " + value, error);
    }
}

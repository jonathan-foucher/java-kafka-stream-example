package com.jonathanfoucher.kafkastream.stream;

import com.jonathanfoucher.kafkastream.data.dto.MovieJsonKey;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class HeaderTransformerSupplier implements ProcessorSupplier<MovieJsonKey, MovieJsonValue, MovieJsonKey, MovieJsonValue> {
    @Override
    public Processor<MovieJsonKey, MovieJsonValue, MovieJsonKey, MovieJsonValue> get() {
        return new HeaderTransformer();
    }
}

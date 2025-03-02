package com.jonathanfoucher.kafkastream.stream;

import com.jonathanfoucher.kafkastream.data.dto.MovieJsonKey;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonValue;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Instant;

public class HeaderTransformer implements Processor<MovieJsonKey, MovieJsonValue, MovieJsonKey, MovieJsonValue> {
    private ProcessorContext<MovieJsonKey, MovieJsonValue> processorContext;

    private static final String KEY_HEADER_NAME = "movie-id";
    private static final String TIME_HEADER_NAME = "time";

    @Override
    public void init(ProcessorContext<MovieJsonKey, MovieJsonValue> processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public void process(Record<MovieJsonKey, MovieJsonValue> movieRecord) {
        MovieJsonKey key = movieRecord.key();
        Headers headers = movieRecord.headers();
        headers.add(KEY_HEADER_NAME, new byte[]{key.getId().byteValue()});
        headers.add(TIME_HEADER_NAME, String.valueOf(Instant.now()).getBytes());
        processorContext.forward(movieRecord);
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}

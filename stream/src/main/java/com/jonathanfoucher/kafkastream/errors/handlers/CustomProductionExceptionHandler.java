package com.jonathanfoucher.kafkastream.errors.handlers;

import com.jonathanfoucher.kafkastream.config.EnvConfig;
import com.jonathanfoucher.pojo.avro.movie.MovieKey;
import com.jonathanfoucher.pojo.avro.movie.MovieValue;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Collections;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Slf4j
public class CustomProductionExceptionHandler implements ProductionExceptionHandler {
    private final EnvConfig envConfig = new EnvConfig();

    @Override
    public ProductionExceptionHandlerResponse handle(ErrorHandlerContext errorHandlerContext, ProducerRecord<byte[], byte[]> producerRecord, Exception exception) {
        Map<String, String> deserializerConfig = Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, envConfig.getSchemaRegistryUrl());

        Deserializer<MovieKey> movieKeyDeserializer = new SpecificAvroDeserializer<>();
        movieKeyDeserializer.configure(deserializerConfig, true);

        Deserializer<MovieValue> movieValueDeserializer = new SpecificAvroDeserializer<>();
        movieValueDeserializer.configure(deserializerConfig, false);

        MovieKey key;
        try {
            key = movieKeyDeserializer.deserialize(producerRecord.topic(), producerRecord.key());
        } finally {
            movieKeyDeserializer.close();
        }

        MovieValue value;
        try {
            value = movieValueDeserializer.deserialize(producerRecord.topic(), producerRecord.value());
        } finally {
            movieValueDeserializer.close();
        }

        log.error("Stream production error for key: {}, value: {}", key, value, exception);
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Just overriding
    }
}

package com.jonathanfoucher.kafkastream.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jonathanfoucher.kafkastream.config.EnvConfig;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonKey;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonValue;
import com.jonathanfoucher.kafkastream.errors.DeserializationException;
import com.jonathanfoucher.kafkastream.errors.StreamProcessingException;
import com.jonathanfoucher.kafkastream.errors.handlers.CustomProductionExceptionHandler;
import com.jonathanfoucher.pojo.avro.movie.MovieKey;
import com.jonathanfoucher.pojo.avro.movie.MovieValue;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;

public class MovieStream {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final EnvConfig envConfig;

    private static final String COMPRESSION_FORMAT = "zstd";

    public MovieStream(EnvConfig envConfig) {
        this.envConfig = envConfig;
    }

    public Properties createSteamConfigs() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, envConfig.getGroupId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envConfig.getBootstrapServer());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, CustomProductionExceptionHandler.class);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_FORMAT);

        String securityProtocol = envConfig.getSecurityProtocol();
        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, envConfig.getSslProtocol());
            properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envConfig.getSslKeystoreType());
            properties.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, envConfig.getSslKeystoreKey());
            properties.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, envConfig.getSslKeystoreCert());
            properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envConfig.getSslTruststoreType());
            properties.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, envConfig.getSslTruststoreCert());
        }

        return properties;
    }

    public Topology createStreamTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> serdeConfigs = new HashMap<>();
        serdeConfigs.put(SCHEMA_REGISTRY_URL_CONFIG, envConfig.getSchemaRegistryUrl());
        serdeConfigs.put(BASIC_AUTH_CREDENTIALS_SOURCE, envConfig.getAuthSource());
        serdeConfigs.put(USER_INFO_CONFIG, envConfig.getUserInfo());
        serdeConfigs.put(AUTO_REGISTER_SCHEMAS, envConfig.getAutoRegisterSchemas());

        Serde<MovieKey> movieKeySerde = new SpecificAvroSerde<>();
        movieKeySerde.configure(serdeConfigs, true);

        Serde<MovieValue> movieValueSerde = new SpecificAvroSerde<>();
        movieValueSerde.configure(serdeConfigs, false);

        builder.<String, String>stream(envConfig.getTopicIn())
                .map((key, value) -> {
                    MovieJsonKey jsonKey = convertStringKeyToJsonKey(key);
                    MovieJsonValue jsonValue = convertStringValueToJsonValue(value);
                    return new KeyValue<>(jsonKey, jsonValue);
                }, Named.as("MAP-CONVERT-STRING-TO-JSON"))
                .process(new HeaderTransformerSupplier(), Named.as("PROCESS-ADD-HEADERS"))
                .map((key, value) -> {
                    try {
                        MovieKey avroKey = convertJsonKeyToAvroKey(key);
                        MovieValue avroValue = convertJsonValueToAvroValue(value);
                        return new KeyValue<>(avroKey, avroValue);
                    } catch (Exception exception) {
                        throw new StreamProcessingException(key, value, exception);
                    }
                }, Named.as("MAP-CONVERT-JSON-TO-AVRO"))
                .to(envConfig.getTopicOut(), Produced.with(movieKeySerde, movieValueSerde));

        return builder.build();
    }

    private MovieJsonKey convertStringKeyToJsonKey(String key) {
        try {
            return objectMapper.readValue(key.getBytes(), MovieJsonKey.class);
        } catch (IOException exception) {
            throw new DeserializationException(key, exception);
        }
    }

    private MovieJsonValue convertStringValueToJsonValue(String value) {
        if (value == null) {
            return null;
        }

        try {
            return objectMapper.readValue(value.getBytes(), MovieJsonValue.class);
        } catch (IOException exception) {
            throw new DeserializationException(value, exception);
        }
    }

    private MovieKey convertJsonKeyToAvroKey(MovieJsonKey key) {
        MovieKey avroKey = new MovieKey();
        avroKey.setId(key.getId());
        return avroKey;
    }

    private MovieValue convertJsonValueToAvroValue(MovieJsonValue value) {
        if (value == null) {
            return null;
        }

        MovieValue avroValue = new MovieValue();
        avroValue.setId(value.getId());
        avroValue.setTitle(value.getTitle());
        avroValue.setReleaseDate(value.getReleaseDate());
        return avroValue;
    }
}

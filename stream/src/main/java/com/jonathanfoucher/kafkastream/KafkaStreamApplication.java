package com.jonathanfoucher.kafkastream;

import com.jonathanfoucher.kafkastream.config.EnvConfig;
import com.jonathanfoucher.kafkastream.healthcheck.Healthcheck;
import com.jonathanfoucher.kafkastream.stream.MovieStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

@Slf4j
public class KafkaStreamApplication {
    private static final EnvConfig envConfig = new EnvConfig();

    public static void main(String[] args) {
        MovieStream movieStream = new MovieStream(envConfig);

        Properties properties = movieStream.createSteamConfigs();
        Topology topology = movieStream.createStreamTopology();
        log.info(topology.describe().toString());

        startKafkaStreams(properties, topology);
    }

    private static void startKafkaStreams(Properties properties, Topology topology) {
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();

        Healthcheck healthcheck = new Healthcheck(kafkaStreams);
        healthcheck.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            healthcheck.stop();
        }));
    }
}

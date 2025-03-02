package com.jonathanfoucher.kafkastream.healthcheck;

import com.jonathanfoucher.kafkastream.errors.HttpServerCreationException;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.net.InetSocketAddress;

@Slf4j
public class Healthcheck {
    private static final int HTTP_SERVER_PORT = 8091;
    private static final String HEALTH_ENDPOINT = "/health";
    private static final int OK_STATUS_CODE = 200;
    private static final int SERVICE_UNAVAILABLE_STATUS_CODE = 503;

    private final KafkaStreams kafkaStreams;
    private HttpServer httpServer;

    public Healthcheck(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public void start() {
        try {
            httpServer = HttpServer.create(new InetSocketAddress(HTTP_SERVER_PORT), 0);
        } catch (IOException exception) {
            throw new HttpServerCreationException(exception);
        }

        httpServer.createContext(HEALTH_ENDPOINT, exchange -> {
            int statusCode = kafkaStreams.state().isRunningOrRebalancing() ? OK_STATUS_CODE : SERVICE_UNAVAILABLE_STATUS_CODE;
            exchange.sendResponseHeaders(statusCode, 0);
            exchange.close();
        });

        httpServer.start();
        log.info("Healthcheck http server started");
    }

    public void stop() {
        httpServer.stop(0);
    }
}

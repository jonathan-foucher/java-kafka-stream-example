package com.jonathanfoucher.kafkastream.healthcheck;

import com.jonathanfoucher.kafkastream.config.EnvConfig;
import com.jonathanfoucher.kafkastream.errors.HttpServerCreationException;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.net.HttpURLConnection.*;

@Slf4j
public class Healthcheck {
    private static final String HEALTH_ENDPOINT = "/health";
    private static final String GET_METHOD = "GET";

    private final KafkaStreams kafkaStreams;
    private final EnvConfig envConfig;
    private HttpServer httpServer;

    public Healthcheck(KafkaStreams kafkaStreams, EnvConfig envConfig) {
        this.kafkaStreams = kafkaStreams;
        this.envConfig = envConfig;
    }

    public void start() {
        try {
            httpServer = HttpServer.create(new InetSocketAddress(envConfig.getHttpServerPort()), 0);
        } catch (IOException exception) {
            throw new HttpServerCreationException(exception);
        }

        httpServer.createContext(HEALTH_ENDPOINT, exchange -> {
            if (exchange.getRequestMethod().equals(GET_METHOD)) {
                int statusCode = kafkaStreams.state().isRunningOrRebalancing() ? HTTP_OK : HTTP_UNAVAILABLE;
                exchange.sendResponseHeaders(statusCode, 0);
            } else {
                exchange.sendResponseHeaders(HTTP_BAD_METHOD, 0);
            }
            exchange.close();
        });

        httpServer.start();
        log.info("Healthcheck http server started");
    }

    public void stop() {
        httpServer.stop(0);
        log.info("Healthcheck http server stopped");
    }
}

package com.jonathanfoucher.kafkastream.healthcheck;

import com.jonathanfoucher.kafkastream.config.EnvConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import static java.net.HttpURLConnection.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HealthcheckTest {
    private static final int HTTP_SERVER_PORT = 8091;
    private static final String HEALTH_ENDPOINT = "/health";
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";

    @Mock
    private KafkaStreams kafkaStreams;
    @Mock
    private EnvConfig envConfig;
    @InjectMocks
    private Healthcheck healthcheck;

    @BeforeEach
    void setup() {
        when(envConfig.getHttpServerPort())
                .thenReturn(HTTP_SERVER_PORT);

        healthcheck.start();
    }

    @AfterEach
    void stop() {
        healthcheck.stop();
    }

    @Test
    void healthcheckWithKafkaStreamsIsRunning() throws IOException {
        // GIVEN
        when(kafkaStreams.state())
                .thenReturn(KafkaStreams.State.RUNNING);

        // WHEN
        int statusCode = getHttpStatusCodeStatusCode(GET_METHOD);

        // THEN
        assertEquals(HTTP_OK, statusCode);
    }

    @Test
    void healthcheckWithKafkaStreamsIsRebalancing() throws IOException {
        // GIVEN
        when(kafkaStreams.state())
                .thenReturn(KafkaStreams.State.REBALANCING);

        // WHEN
        int statusCode = getHttpStatusCodeStatusCode(GET_METHOD);

        // THEN
        assertEquals(HTTP_OK, statusCode);
    }

    @Test
    void healthcheckWithKafkaStreamsIsNotRunning() throws IOException {
        // GIVEN
        when(kafkaStreams.state())
                .thenReturn(KafkaStreams.State.NOT_RUNNING);

        // WHEN
        int statusCode = getHttpStatusCodeStatusCode(GET_METHOD);

        // THEN
        assertEquals(HTTP_UNAVAILABLE, statusCode);
    }

    @Test
    void healthcheckWithKafkaStreamsRunningAndWrongHttpMethod() throws IOException {
        // WHEN
        int statusCode = getHttpStatusCodeStatusCode(POST_METHOD);

        // THEN
        assertEquals(HTTP_BAD_METHOD, statusCode);
    }

    private int getHttpStatusCodeStatusCode(String method) throws IOException {
        URL url = URI.create("http://localhost:" + HTTP_SERVER_PORT + HEALTH_ENDPOINT).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        int statusCode = connection.getResponseCode();
        connection.disconnect();
        return statusCode;
    }
}

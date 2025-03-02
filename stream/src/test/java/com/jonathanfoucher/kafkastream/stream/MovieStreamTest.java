package com.jonathanfoucher.kafkastream.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jonathanfoucher.kafkastream.config.EnvConfig;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonKey;
import com.jonathanfoucher.kafkastream.data.dto.MovieJsonValue;
import com.jonathanfoucher.kafkastream.errors.DeserializationException;
import com.jonathanfoucher.pojo.avro.movie.MovieKey;
import com.jonathanfoucher.pojo.avro.movie.MovieValue;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MovieStreamTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<MovieKey, MovieValue> outputTopic;

    @Mock
    private EnvConfig envConfig;
    @Mock
    private HeaderTransformer headerTransformer;
    @InjectMocks
    private MovieStream movieStream;

    private static final ObjectMapper objectMapper;

    private static final String AUTH_SOURCE = "USER_INFO";
    private static final String BOOTSTRAP_SERVER = "localhost:9093";
    private static final String GROUP_ID = "movie-stream";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8181";
    private static final String TOPIC_IN = "kafka_example_movie_json";
    private static final String TOPIC_OUT = "kafka_example_movie_avro";
    private static final Boolean AUTO_REGISTER_SCHEMAS = Boolean.TRUE;

    private static final String KEY_HEADER_NAME = "movie-id";
    private static final String TIME_HEADER_NAME = "time";

    private static final Long MOVIE_ID = 27L;
    private static final String MOVIE_TITLE = "Some movie title";
    private static final LocalDate MOVIE_RELEASE_DATE = LocalDate.of(2022, 2, 24);

    static {
        objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        objectMapper.registerModule(new JavaTimeModule());
    }

    @BeforeEach
    void setup() {
        when(envConfig.getAuthSource()).thenReturn(AUTH_SOURCE);
        when(envConfig.getBootstrapServer()).thenReturn(BOOTSTRAP_SERVER);
        when(envConfig.getGroupId()).thenReturn(GROUP_ID);
        when(envConfig.getSchemaRegistryUrl()).thenReturn("mock://" + SCHEMA_REGISTRY_URL);
        when(envConfig.getTopicIn()).thenReturn(TOPIC_IN);
        when(envConfig.getTopicOut()).thenReturn(TOPIC_OUT);
        when(envConfig.getAutoRegisterSchemas()).thenReturn(String.valueOf(AUTO_REGISTER_SCHEMAS));

        testDriver = new TopologyTestDriver(movieStream.createStreamTopology(), movieStream.createSteamConfigs());
        inputTopic = testDriver.createInputTopic(TOPIC_IN, new StringSerializer(), new StringSerializer());

        Map<String, String> serdeConfigs = new HashMap<>();
        serdeConfigs.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://" + SCHEMA_REGISTRY_URL);

        Deserializer<MovieKey> keyDeserializer = new SpecificAvroDeserializer<>();
        keyDeserializer.configure(serdeConfigs, true);
        Deserializer<MovieValue> valueDeserializer = new SpecificAvroDeserializer<>();
        valueDeserializer.configure(serdeConfigs, false);
        outputTopic = testDriver.createOutputTopic(TOPIC_OUT, keyDeserializer, valueDeserializer);
    }

    @AfterEach
    void close() {
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_URL);
        testDriver.close();
    }

    @Test
    void processReceivedMovie() throws JsonProcessingException {
        // GIVEN
        MovieJsonKey movieKey = initMovieKey();
        MovieJsonValue movieValue = initMovieValue();

        // WHEN
        inputTopic.pipeInput(objectMapper.writeValueAsString(movieKey), objectMapper.writeValueAsString(movieValue));

        // THEN
        assertFalse(outputTopic.isEmpty());
        TestRecord<MovieKey, MovieValue> result = outputTopic.readRecord();

        assertNotNull(result);

        Headers headers = result.getHeaders();
        checkHeaders(headers);

        MovieKey key = result.getKey();
        assertNotNull(key);
        assertEquals(MOVIE_ID, (Long) key.getId());

        MovieValue value = result.getValue();
        assertNotNull(value);
        assertEquals(MOVIE_ID, (Long) value.getId());
        assertEquals(MOVIE_TITLE, String.valueOf(value.getTitle()));
        assertEquals(MOVIE_RELEASE_DATE, value.getReleaseDate());
    }

    @Test
    void processReceivedMovieTombstone() throws JsonProcessingException {
        // GIVEN
        MovieJsonKey movieKey = initMovieKey();

        // WHEN
        inputTopic.pipeInput(objectMapper.writeValueAsString(movieKey), objectMapper.writeValueAsString(null));

        // THEN
        assertFalse(outputTopic.isEmpty());
        TestRecord<MovieKey, MovieValue> result = outputTopic.readRecord();

        assertNotNull(result);

        Headers headers = result.getHeaders();
        checkHeaders(headers);

        MovieKey key = result.getKey();
        assertNotNull(key);
        assertEquals(MOVIE_ID, (Long) key.getId());

        MovieValue value = result.getValue();
        assertNull(value);
    }

    @Test
    void processReceivedMovieWithKeyDeserializationError() {
        // GIVEN
        MovieJsonValue movieValue = initMovieValue();

        // WHEN / THEN
        assertThatThrownBy(() -> inputTopic.pipeInput("wrong_key", objectMapper.writeValueAsString(movieValue)))
                .hasCauseInstanceOf(DeserializationException.class)
                .hasStackTraceContaining("JSON deserialization error: wrong_key");

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void processReceivedMovieWithValueDeserializationError() {
        // GIVEN
        MovieJsonKey movieKey = initMovieKey();

        // WHEN / THEN
        assertThatThrownBy(() -> inputTopic.pipeInput(objectMapper.writeValueAsString(movieKey), "wrong_value"))
                .hasCauseInstanceOf(DeserializationException.class)
                .hasStackTraceContaining("JSON deserialization error: wrong_value");

        assertTrue(outputTopic.isEmpty());
    }

    private MovieJsonKey initMovieKey() {
        MovieJsonKey movieKey = new MovieJsonKey();
        movieKey.setId(MOVIE_ID);
        return movieKey;
    }

    private MovieJsonValue initMovieValue() {
        MovieJsonValue movieValue = new MovieJsonValue();
        movieValue.setId(MOVIE_ID);
        movieValue.setTitle(MOVIE_TITLE);
        movieValue.setReleaseDate(MOVIE_RELEASE_DATE);
        return movieValue;
    }

    private void checkHeaders(Headers headers) {
        assertNotNull(headers);
        Header[] headerArray = headers.toArray();
        assertEquals(2, headerArray.length);

        assertNotNull(headerArray[0]);
        assertEquals(KEY_HEADER_NAME, headerArray[0].key());
        ByteBuffer byteBuffer = ByteBuffer.wrap(headerArray[0].value());
        assertEquals(MOVIE_ID, Long.valueOf(byteBuffer.getLong()));

        assertNotNull(headerArray[1]);
        assertEquals(TIME_HEADER_NAME, headerArray[1].key());
        String dateString = new String(headerArray[1].value());
        assertTrue(Instant.parse(dateString).isBefore(Instant.now()));
        assertTrue(Instant.parse(dateString).isAfter(Instant.now().minusSeconds(10)));
    }
}

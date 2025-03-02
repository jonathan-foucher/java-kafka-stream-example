## Introduction
This project is an example of a Kafka stream with Java and Avro format using a schema registry.

The repository contains two Spring Boot projects and a simple Java project (the Kafka stream) :
- a project to generate the required pojo from Avro
- a kafka json producer fed by a controller (REST API)
- a kafka stream that convert the json object to avro

## Run the project
### POJO
First you will need to generate the POJO classes :
```
mvn clean install -f kafka-pojo/avro/movie/pom.xml
```

### Kafka environment
To deploy the kafka required environment you will need docker installed and run the `docker/docker-compose.yml` file.

It will launch different containers:
- zookeeper
- kafka
- schema-registry
- akhq: a browser GUI to check out topics, messages and schemas
- init-kafka: init container to create the required Kafka topic and schemas


```
docker-compose -f docker/docker-compose.yml up -d
```

You will be able to access akhq on [this url](http://localhost:8190/)

### Application
Once the Kafka environment started and healthy, you can start the producer and the Kafka stream.

For the Kafka stream you will have to set some environment variables:
- AUTH_SOURCE=USER_INFO
- BOOTSTRAP_SERVER=localhost:9093
- GROUP_ID=movie-stream
- SCHEMA_REGISTRY_URL=http://localhost:8181
- TOPIC_IN=kafka_example_movie_json
- TOPIC_OUT=kafka_example_movie_avro
- HTTP_SERVER_PORT=8091

The project also have an optional SSL config as example.


The kafka producer allows you to send json messages on the first topic, then you can check the output with akhq.

Save a movie
```
curl --request POST \
  --url http://localhost:8090/kafka-producer/movies \
  --header 'Content-Type: application/json' \
  --data '{
	"id": 26,
	"title": "Some movie title",
	"release_date": "2022-02-26"
}'
```

Delete a movie
```
curl --request DELETE \
  --url http://localhost:8090/kafka-producer/movies/26
```

The Kafka stream has a healthcheck endpoint.
It will answer a 200 status code if the Kafka stream is running or a 503 otherwise.
```
curl --request GET \
  --url http://localhost:8091/health
```

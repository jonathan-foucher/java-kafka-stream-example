## Introduction
This project is an example of Kafka streaming with Java.

The repository contains three projects :
- a project to generate the required pojo from Avro
- a kafka producer fed by a controller (REST API)
- a kafka stream that consumes and transforms the data, then push it to another topic

## Run the project
### POJO
First you will need to generate the POJO classes :
```
mvn clean install -f kafka-pojo/avro/movie/pom.xml
```

### Kafka environment
To deploy the kafka required environment you will need docker installed and run the `docker/docker-compose.yml` file.

It will launch different containers:
- kafka
- schema-registry
- kafka-ui: a browser GUI to check out topics, messages and schemas
- init-kafka: init container to create the required Kafka topic and schemas


```
docker-compose -f docker/docker-compose.yml up -d
```

You will be able to access kafka-ui on [this url](http://localhost:9090/)

### Application
Once the Kafka environment started and healthy, you can start the Spring Boot producer and the Java kafka stream and try them out.

For the kafka stream, you need to provide those environment variables
```
GROUP_ID=kstream_group_id;BOOTSTRAP_SERVER=localhost:9094;TOPIC_IN=kafka_example_movie_json;TOPIC_OUT=kafka_example_movie_avro;SCHEMA_REGISTRY_URL=http://localhost:8181;AUTH_SOURCE=USER_INFO;HTTP_SERVER_PORT=8091;
```

You can use those curl to feed the entry topic with the producer:

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

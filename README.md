# Data Stream Processing and Analytics

## Getting Started

- Add data files into the `data/` folder (1K)
- Run the cleanup scripts using Python
  - `python stream_cleaning.py`
- Download the flink version to use from the official mirror and store it in the project root
  - Default config: https://archive.apache.org/dist/flink/flink-1.8.0/flink-1.8.0-bin-scala_2.11.tgz
- Override any of the docker build arguments if necessary
- Start the Kafka/Zookeeper/WebUI infrastructure by running `scripts/run_infrastructure.sh`
  - The Kafka WebUI will be available on `localhost:8080`
  - Setup the Kafka cluster using `kafka:9092`
- Initialize necessary input and output topics in Kafka
  - Run `scripts/run_ropic_creation.sh`
- Run the consumers using `scripts/run_consumers.sh`
  - This starts all the analytics tasks in job clusters in separate containers
  - Active post statistics Flink UI: `localhost:8081`
  - Recommendations Flink UI: `localhost:8082`
  - Anomalies Flink UI: `localhost:8083`
- Run the producers using `scripts/run_producers.sh`
  - This starts producing inputs to all three Kafka input topics simultaneously (post, comment, like)

### Stream Producer

Post Stream Producer

```
-file ./../data/1k-users-sorted/streams/post_event_stream.csv
-schema ./../data/schema/avro/post.avsc
-topic post
-kafkaserver 127.0.0.1:9092
-speedup 60
-rdelay 5
-sdelay 600
-seed 1
-worker 2
```

Likes Stream Producer

```
-file ./../data/1k-users-sorted/streams/likes_event_stream.csv
-schema ./../data/schema/avro/like.avsc
-topic like
-kafkaserver 127.0.0.1:9092
-speedup 60
-rdelay 5
-sdelay 600
-seed 2
-worker 2
```

Comment Stream Producer

```
-file ./../data/1k-users-sorted/streams/comment_event_stream_cleaned.csv
-schema ./../data/schema/avro/comment.avsc
-topic comment
-kafkaserver 127.0.0.1:9092
-speedup 60
-rdelay 5
-sdelay 600
-seed 3
-worker 2
```

### Avro Generating Code

```
java -jar ./data/schema/avro/avro-tools-1.8.2.jar compile -string schema data/schema/avro social-network-analysis/target/generated-sources/avro
```

## Troubleshooting

- To connect to Kafka from the host machine, use `localhost:29092`. `kafka:9092` is only available inside Docker containers.

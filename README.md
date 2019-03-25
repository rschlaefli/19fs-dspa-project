# Data Stream Processing and Analytics

## Getting Started

- Add data files into the `data/` folder
- Download the flink version to use from the official mirror and store it in the project root
  - Default config: https://archive.apache.org/dist/flink/flink-1.7.2/flink-1.7.2-bin-scala_2.11.tgz
- Override any of the docker build arguments if necessary
- Run `docker-compose up --build` to build an run the consumer container

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
-file ./../data/1k-users-sorted/streams/comment_event_stream.csv
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
java -jar ./data/schema/avro/avro-tools-1.8.2.jar compile schema data/schema/avro social-network-analysis/target/generated-sources/avro
```

# Data Stream Processing and Analytics

## Getting Started

### Stream Producer

Post Stream Producer
```
-file ./../../data/1k-users-sorted/streams/post_event_stream.csv
-schema ./../../schema/avro/post.avsc
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
-file ./../../data/1k-users-sorted/streams/likes_event_stream.csv
-schema ./../../schema/avro/like.avsc
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
-file ./../../data/1k-users-sorted/streams/comment_event_stream.csv
-schema ./../../schema/avro/comment.avsc
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
java -jar ./schema/avro/avro-tools-1.8.2.jar compile schema schema/avro consumer/analysis-consumer/target/generated-sources/avro
```

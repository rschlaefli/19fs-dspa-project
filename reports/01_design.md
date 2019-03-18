# DSPA 2019 - Design Document

Roland Schlaefli, 12-932-398 and Nicolas Kuechler, 14-712-129

The processing engine of choice for the streaming application is Apache Flink. The main drivers behind this choice were the maturity and popularity (especially in the industry) of the Java ecosystem and Apache Flink itself.

Apart from Flink, Apache Kafka is used as an auxiliary system for both input and output streams. Results are visualized in a web UI with a foundation on existing open-source solutions for consumption of Kafka topics. While the current design implementation does not include any state backend or external key-value storage system, this might change in future iterations, where e.g. RocksDB could be a useful addition.

The figure below provides an overview of the overall architecture of the system:

![architecture-overview.png](architecture-overview.png)

## Input & Preprocessing

The input pipeline consists of three custom Kafka producers that simulate realistic streaming sources. The streams are read from files and produces to separate Kafka topics (`post`, `comment`, and `like`). Each event is scheduled such that it is produced proportional to the event time plus a bounded random delay. A speedup parameter can be used to control the speed of the stream. Data transmitted through Kafka is encoded and decoded using Avro schemas, simplifying data ingestion by the Flink consumer.

Using Kafka increases the latency of the system but brings the advantage of failure handling and replay capabilities, overall leading to a more reliable system considering the application context. Due to the reliance on event time for processing, the introduction of a bounded random delay can lead to events that arrive out of order. To guarantee correct semantics for time-sensitive operators, periodic watermarks are generated at the source of the consumer based on the same upper bound of random delay as is set in the producer. This necessarily increases the latency but is inevitable for correct results.

## Task 1 - Active Posts Statistics

The basic approach of the post statistics computation is to first map comments to a post id, such that all streams can be merged into a single stream with a common schema keyed by post id. Based on all three input streams, all information except a triple consisting of post id, type (post, reply, comment, or like), and a person id along with the event time, can be directly discarded. No additional information or static data is required to compute the results.

Based on the prepared input stream, events are assigned to sliding windows of 12 hour length and 30 minute step size. If active post statistics were to be kept for longer than 12 hours, a global state could store TODO: . Incremental aggregation functions with an internal state can then be applied to compute comment, reply, and unique people counts. To achieve unique people counts, an internal set of known person ids will need to be maintained in addition to the mapping of post id to people count.

- TODO: keeping a global state that persists active streams that are out of window
  - cleanup?
- TODO: what if a post is reactivated?
- ...

## Task 2 - Recommendation Service

The high-level idea of the friends recommendation computation is to generate a vector representation of each user (i.e., a user embedding) based on two main components:

- User activity over the last 4 hours (tags of created posts, liked posts or commented posts, tags of the forum a user interacted with, posting/commenting from a place, possibly even topics of the content)
- Static information (work at organization, study at university, interest in tags, interest in tags of the same class, membership in forum, speaking a language)

Given user embeddings and a similarity metric (e.g. Euclidean distance, cosine similarity, ...), the top 5 most similar people can be calculated for every person. Inactive users or already existing friendships are filtered out to provide meaningful recommendations. In a first iteration, the user embeddings are purely based on simple counts of interactions with the different categories (tags, tag classes, places, languages, ...). In a second iteration, this idea can possibly be extended to a similarity learning method trying to ensure that people that are actually friends also receive a high similarity.

The recommendations task receives all three input streams which are enriched with static data at certain operators. The required static data is loaded into memory from the respective file in the open() function of the rich operator.

## Task 3 - Anomaly Detection

TODO: one-sentence summary

The Unusual activity detection task receives also all three input streams which are enriched with static data at certain operators similar to the recommendations task, however the used static data might varies between the two tasks.

The high-level idea is to extract features from the stream such

## Output & Postprocessing

The results of the streaming analytics tasks are all written to separate Kafka topics. Since the system already uses Kafka in the input pipeline, there is little overhead to also use it for the output but brings the advantage of introducing decoupling between the processing layer and the visualization layer as well as all the other advantages of Kafka.

The plan is to present the results to the user in an open source web UI such as https://github.com/Landoop/kafka-topics-ui which is a simple Kafka consumer.

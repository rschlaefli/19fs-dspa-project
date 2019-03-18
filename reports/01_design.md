# DSPA 2019 - Design Document

Roland Schlaefli, 12-932-398
Nicolas Kuechler, 14-712-129

The processing engine of the streaming application is Apache Flink. The main drivers behind this choice were the maturity and popularity of the Java ecosystem and Apache Flink itself (especially in the industry). However, Flink also brings other advantages ... TODO:

Apart from Flink, the main auxiliary system is Apache Kafka, which is used for both input and output streams. In addition to Kafka, results are visualized in a web UI based on existing open-source solutions.
The current design implementation does not include any state backend or external key-value storage system. However, this might change in future iterations, where e.g. RocksDB could be a useful addition.

The figure below gives an overview of the architecture of the system.
There are three parts Input & Pre-processing, Processing, and Output & Post-processing
which are described in the following in more detail.

TODO: Insert Image

## Input & Pre-processing

The input pipeline consists of three custom Kafka producers simulating a realistic
streaming source by parsing the files containing the streams and writing
each of them to a separate Kafka topic (post, comment, like).
Each event is scheduled such that it is written to Kafka proportional to the
event time plus a bounded random delay. A speedup parameter can be used to control
the speed of the stream.
Data transmitted through Kafka is encoded and decoded using Avro schemas, which among other advantages simplify the
Using Kafka increases the latency of the system but brings the
advantage of failure handling, redundancy, and persistence on disk; overall leading
to a more stable system. Additionally, it provides the necessary replay functionality.

The introduction of the bounded random delay can lead to events arriving out of
order because the notion of event time is used in all the streaming programs.
To guarantee correct semantics for time-sensitive operators,
periodic watermarks are generated at the source of the consumer using the same
bounded random delay as the producer.
This necessarily increases the latency but is inevitable for correct results.

## Processing

### Task 1 - Active Posts Statistics

#### Input Format

The active posts statistics task receives all three input streams but can
directly discard all information except a triple consisting of post id, type (post, reply, comment, like) and a person id along with the event time.
No additional information is required to compute the results.

#### Overview

TODO:

- manage a mapping between comment id and post id
- stateful window keeping counts of the number of comments and replies and list of personId

### Task 2 - Recommendation Service

#### Input Format

The recommendations task receives all three input streams which are enriched
with static data at certain operators.
The required static data is loaded into memory from the respective file in the open() function of the rich operator.

#### Overview

The high-level idea is to generate a vector representation of each user (user embedding) based on two components:

- user activity during the last 4 hours (tags of created posts, liked posts or commented posts,
  tags of the forum a user interacted with, posting/commenting from a place, possibly even topics of the content)
- static information (work at organization, study at university, interest in tags, interest in tags of the same class, membership in forum, speaking a language)

Given user embeddings and a similarity metric (e.g. Euclidean distance, cosine similarity, ...),
the top 5 most similar people can be calculated for every person.
Inactive users or already existing friendships are filtered out to provide meaningful recommendations.

In a first iteration, the user embeddings are purely based on simple counts of interactions with the different categories (tags, tag classes, places, languages, ...). In a second iteration, this idea can possibly be extended to a similarity learning method trying to ensure that people that are actually friends also receive a high similarity.

### Task 3 - Anomaly Detection

#### Input Format

The Unusual activity detection task receives also all three input streams which are enriched
with static data at certain operators similar to the recommendations task,
however the used static data might varies between the two tasks.

#### Overview

The high-level idea is to extract features from the stream such

## Output & Postprocessing

The results of the streaming analytics tasks are all written to separate Kafka topics. Since the system already uses Kafka in the input pipeline, there is little overhead to also use it for the output but brings the advantage of introducing decoupling between the processing layer and the visualization layer as well as all the other advantages of Kafka.

The plan is to present the results to the user in an open source web UI such as https://github.com/Landoop/kafka-topics-ui which is a simple Kafka consumer.

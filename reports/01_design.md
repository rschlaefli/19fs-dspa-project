# DSPA 2019 - Design Document

Roland Schlaefli, 12-932-398
Nicolas Kuechler, 14-712-129

TODO: Briefly describe what you are planning to build and how you are planning to build it.

TODO: Briefly argue why your design solves the given problems and describe its advantages over alternative approaches.

## Input & Preprocessing

TODO: Where does your application read input from? How does this choice affect latency and semantics (order)?

All streaming input processed in our application is consumed from separate Kafka topics. The data generation and preprocessing for these topics is handled by several Kafka producer classes that ensure data is generated in a reasonable fashion. Events are scheduled such that they are written to Kafka proportionally to the actual creation timestamp of the event (after application of a speedup factor). All data transmitted through Kafka is encoded and decoded using Avro schemas, allowing us to more easily extract data on the consumer side.

The latency of our application is increased by our choice of a Kafka pipeline, as Kafka implements measures for failure handling, redundancy, and persistence on disk. Furthermore, using Kafka means that there is an additional transmission over the network in-between Kafka and Flink, as these services normally would run on separate clusters. The latency will necessarily be increased by the random bounded delay that needs to be considered when watermarking, as time-sensitive operators only run once a watermark has become available. Correct semantics of the event streams are ensured by our approach of computing watermarks based on the bounded delay, as well as our usage of event time for computations. The usage of Kafka does not influence the semantics in a direct way, as it does not perform any preprocessing or reordering of the events based on their timestamp (in our implementation).

- x kafka topics with producers for data generation
  - x producer reading stream file and scheduling events that write event to kafka proportional to created timestamp of event
  - x avro schemas
- read static table data via RichMapFunction to enrich stream where necessary
- x latency?
  - x watermarking by max. delay will increase latency for time-sensitive operators
  - x latency is increased by kafka due to failsafe measures, redundancy, disk operations
  - x data is transferred via network
- x semantics?
  - x event time of events taken for computations
  - x correct ordering ensured by application of watermarks after bounded delay

TODO: What is the format of the input streams for each of the analytics tasks?

All analytics tasks are fed with at least the three available input streams (using three separate Kafka topics). The pure analytics task (#1) does not make use of any additional data, as all results can be directly computed from the stream contents. The recommendations (#2) will be performed based on streaming data that has been enriched with static data using enrichment functions like `RichMap` and other processing functions. This basic structure also applies to anomaly detection (#3), where we make use of the same enriched streams, although the specific data enriched might differ. The streams ingested from Kafka are decoded from Avro schemas.

- x analytics (1): separate streams from separate kafka topics, no static data, based on avro schemas
- x recommendation (2): separate streams, enrich streams with necessary static data
  - x enrichment in operators or in preprocessing?
- x anomalies (3): same as (2)?

## Processing

TODO: What is your stream processor of choice? What features guided your choice?

The main processing engine for our streaming application is Apache Flink. Our main drivers were our familiarity with Java, as well as the maturity and popularity of the Java ecosystem and Apache Flink itself (especially in the industry). However, we think that Flink also brings other advantages ... TODO:

- x flink!
  - x java (familiarity with language)
  - x maturity & popularity in industry
  - TODO: google advantages of flink

TODO: Will you use other auxiliary systems / tools, e.g. for intermediate data?

One of the main auxiliary systems we use is Apache Kafka, which we use for consuming input streams, as well as producing output streams. Indirectly, this also means that we have dependencies on Zookeeper and all other systems that Kafka itself depends on. In addition to Kafka, we visualize outputs that have been stored in a Kafka stream by means of a web ui based on existing open-source solutions. Our current design implementation does not include any state backend or external key-value storage system. However, this might change in future iteratios, where we might come to the conclusion that e.g. RocksDB would be a useful addition.

- x input data from kafka, output to kafka
  - x including zookeeper, etc.
- x web ui as kafka consumer for output visualization
  - x existing oss project
- x any external key-value store? state backend?
  - x probably not, maybe rocksdb for state storage

## Output & Postprocessing

TODO: Where does your application output results? How are these results shown to the user?

- x output to kafka topics for each "task"
- x show results in simple web ui with kafka consumer

The computation results of our stream analytics tasks are all output to separate Kafka topics. As our streaming application already consumes all of its input from Kafka topics, we decided that producing and storing all of our output in a Kafka topic would be a good approach. This allows us to decouple the computation and processing layer from the vsualization layer, as visualizations will be based purely on Kafka topic contents.

## Statistics

- ...

## Recommendation

- Based on posts and comments: apply "category" modeling
  - tags (of post and maybe forum), language, forum, and place (static data)
    - enrich tags with static information from tagclasses (RichMap sth.)
    - forum maybe as category of its own, maybe just for tags
  - maybe content of post or comment (modelet in static categories)
  - only fixed categorizations in our generated data case
- Count interactions of users with these categories
  - based on all streams (incl. likes)
  - within 4 hour window
- compute intra-user similarity by application of distance measures
  - users with similar interaction counts for categories are more similar

## Anomaly Detection

- ...

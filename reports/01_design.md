# DSPA 2019 - Design Document

Roland Schlaefli, 12-932-398
Nicolas Kuechler, 14-712-129

TODO: Briefly describe what you are planning to build and how you are planning to build it.

TODO: Briefly argue why your design solves the given problems and describe its advantages over alternative approaches.

## Input & Preprocessing

TODO: Where does your application read input from? How does this choice affect latency and semantics (order)?

- kafka topics with producers for data generation
  - producer reading stream file and scheduling events that write event to kafka proportional to created timestamp of event
  - avro schemas
- read static table data via RichMapFunction to enrich stream where necessary
- latency?
  - watermarking by max. delay will increase latency for time-sensitive operators
  - latency is increased by kafka due to failsafe measures, redundancy, disk operations
  - data is transferred via network
- semantics?
  - event time of events taken for computations
  - correct ordering ensured by application of watermarks after bounded delay

TODO: What is the format of the input streams for each of the analytics tasks?

- analytics (1): separate streams from separate kafka topics, no static data, based on avro schemas
- recommendation (2): separate streams, enrich streams with necessary static data
  - enrichment in operators or in preprocessing?
- anomalies (3): same as (2)?

## Processing

TODO: What is your stream processor of choice? What features guided your choice?

- flink!
  - java (familiarity with language)
  - maturity & popularity in industry
  - TODO: google advantages of flink

TODO: Will you use other auxiliary systems / tools, e.g. for intermediate data?

- input data from kafka, output to kafka
  - including zookeeper, etc.
- web ui as kafka consumer for output visualization
  - existing oss project
- any external key-value store? state backend?
  - probably not, maybe rocksdb for state storage

## Output & Postprocessing

TODO: Where does your application output results? How are these results shown to the user?

- output to kafka topics for each "task"
- show results in simple web ui with kafka consumer

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

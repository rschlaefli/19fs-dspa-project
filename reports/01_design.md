# DSPA 2019 - Design Document

Roland Schlaefli, 12-932-398
Nicolas Kuechler, ...

TODO: Briefly describe what you are planning to build and how you are planning to build it.

TODO: Briefly argue why your design solves the given problems and describe its advantages over alternative approaches.

## Input & Preprocessing

TODO: Where does your application read input from? How does this choice affect latency and semantics (order)?

- kafka topics with producers for data generatiion
- avro schemas
- latency?
  - ...
- semantics?
  - ...
- ...

TODO: What is the format of the input streams for each of the analytics tasks?

- preprocessing? enrichment with static data?
  - in kafka? other places?
- ...

## Processing

TODO: What is your stream processor of choice? What features guided your choice?

- flink!
  - java
  - maturity
  - ...

TODO: Will you use other auxiliary systems / tools, e.g. for intermediate data?

- any intermediate data in kafka?
  - maybe?
- any external key-value store?
  - probably not
- ...

## Output & Postprocessing

TODO: Where does your application output results? How are these results shown to the user?

- output to kafka topics?
- show results in simple web ui with kafka js consumer?
- ...

# Project Demo

This file contains a suggested course of events/commands when running the application.

## Task 0: Data Cleaning

- Make sure the data folders (`1k-users-sorted` and `10k-users-sorted`) are placed inside the `data/` directory
- Run the data cleanup script using `python scripts/stream_cleaning.py` (Python 3)
  - Takes around 8 minutes to process both 1k and 10k datasets
  - Needs to be done a single time before the first launch of the system
- If Windows is used, see README for troubleshooting and additional cleanup for Windows/Unix file-endings

## Task 1: Active Post Statistics

- Bootstrap the architecture for Task 1
  - `scripts/_start.sh --data-dir 10k-users-sorted --speedup 2400 --parallelism 4 --maxdelaysec 600 --task statistics`
  - Slower speedup and more parallelism, as lots of data is produced
  - Use `--build` to start an intial build of all docker images (this can take a few minutes)
  - Tasks / Clusters take a while to bootstrap and output initial data
- Once the bootstrapping has finished, the following services are available for Task 1
  - Flink Web UI at `localhost:8081`
  - Frontend at `localhost:4000`
- Only small portion of data is cached in memory (FIFO, ~8 hours)
  - When data is pruned from the Frontend FIFO cache, slight inconsistencies in the frontend might occur especially at the borders of windows
  - Data in Kafka topics is consistent. To directly view the Kafka topics, run `scripts/start_sh` with `--kafka-ui`, which makes available a Kafka UI at `localhost:8080`
    - Cluster access in the UI needs to be setup according to the README instructions
  - Alternatively, use a console consumer like:
    - `~/apps/kafka_2.11-2.1.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic active-posts-out --from-beginning`
  - The input topics `post`, `comment`, and `like` are available in the same way

## Tasks 2 & 3: Recommendations and Unusual Activity Detection

- Bootstrap the architecture for Task 2 & 3
  - `scripts/_start.sh --data-dir 10k-users-sorted --speedup 3600 --parallelism 2 --maxdelaysec 600 --task recommendations+anomalies`
  - Faster speedup as data is more selectively produced by these task
  - Tasks / Clusters take a while to bootstrap and output initial data
- Once the bootstrapping has finished, the following services are available for Task 2 & 3
  - Flink Web UIs at `localhost:8082` and `localhost:8083`
  - Frontend at `localhost:4000`
- Kafka topics can be directly accessed via the Kafka UI as described for Task 1. Console consumers can be run like:
  - `~/apps/kafka_2.11-2.1.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic recommendations-out --from-beginning`
  - `~/apps/kafka_2.11-2.1.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic anomalies-out --from-beginning`
  - The input topics `post`, `comment`, and `like` are available in the same way

## Notes

- When running `1k-users-sorted` with recommendations, make sure to pass other person ids, as the default ids are matched to `10k-users-sorted`. We suggest using `--person-ids "294 166 344 740 724 722 273 345 658 225"`.
  - `scripts/_start.sh --task recommendations+anomalies --parallelism 2 --data-dir 1k-users-sorted --maxdelaysec 600 --speedup 3600 --person-ids "294 166 344 740 724 722 273 345 658 225"`

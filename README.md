# Data Stream Processing and Analytics

TODO: add architecture image
TODO: general description
TODO: dockerization (packaging)

## Limitations and Performance

To run the entire application stack including all three analytics tasks and data producers at the same time, a machine with at least 16GB of ram is necessary. This is caused by the wide range of tasks that need to run simultaneously. Additionally, it is preferable to have at least a quad-core processor, as the architecture might not perform at its best otherwise.

## Dependencies

The following dependencies are needed to prepare for and run the streaming application:

- Docker from ~18.04
  - WEBLINK
- Python 3
  - Install the `tqdm` Python package (`pip install tqdm`)
- Flink Binary in `.tgz` format
  - Download the Flink 1.8.0 dependency to use from the official mirror and store it in the project root
  - Available on https://archive.apache.org/dist/flink/flink-1.8.0/flink-1.8.0-bin-scala_2.11.tgz
  - This will be bundled into the docker images, so naming is important

## Data Preparation

The data files that are to be fed into the streaming application need to be prepared as follows:

- Add data files into the `data/` folder
  - Unpacked into subfolders called `1k-users-sorted` and `10k-users-sorted`
- Run the cleanup script with Python (`python scripts/stream_cleaning.py`)
  - This will clean the data files for XXX

## Running the Streaming Application

Once the dependencies have been installed and the data is in the correct location/format, run the application as follows:

- Run `scripts/_start.sh`, which does the following in its default configuration (see below for parametrization):
  - Bootstraps an entire Kafka/Flink infrastructure
  - Starts FLink job clusters for the three streaming analytics tasks
  - Once everything is ready, runs three stream producers for the cleaned input topics (post/comment/like)
  - TODO: Starts up the custom web interface that visualizes the analytics outputs (frontend/backend/cache)

After the application has been successfully bootstrapped, the following services are available:

- Flink WebUIs for all three tasks
  - Task 1 - Active Post Statistics at `localhost:8081`
  - Task 2 - Friend Recommendations at `localhost:8082`
  - Task 3 - Unusual Activities at `localhost:8083`
- The custom visualization frontend at `localhost:4000`
  - The results of the different tasks are updated every second (or 2 seconds for the statistics task)
- TODO: Add Kafka WebUI?

## Parametrization

The streaming application can be parametrized by several means:

- Command line arguments to `scripts/_start.sh`
  - To run a specific analytics task instead of all three at once:
    - `scripts/_start.sh statistics`
    - `scripts/_start.sh recommendations`
    - `scripts/_start.sh anomalies`
  - To increase the parallelism of the analytics tasks:
    - Defaults to parallelism=1 for performance reasons
    - TODO: add a CLI param for this
  - To change the directory where data is to be read from:
    - Defaults to `./data/10k-users-sorted`
    - TODO: add a CLI param for this
    - The data needs to have been cleaned up with the Python scripts!
    - Add the directory without a trailing slash!
  - To change the maximum bounded delay of the producers and analytics tasks:
    - Defaults to 600
    - TODO: unit? seconds, milliseconds?
    - TODO: add a CLI param for this
  - TODO: rdelay
  - TODO: speedup

## Troubleshooting

- To connect to Kafka from the host machine, use `localhost:29092`.
  - `kafka:9092` is only available inside Docker containers.
- If there have been any code changes at all, make sure to run `_start.sh` with the `--build` parameter. This ensures that all the changes code is also actually bundled into the docker images before running them.
- On Windows, there might be issues with files not being found inside most of the Docker containers
  - Most often, this occurs to the data and other files being stored with Windows file-endings, which breaks when reading them in Linux
  - To convert these files to use the appropriate Linux file-endings, run `scripts/file_endings_win.bat` from the repository root

## Command Line Operations

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

### Avro Schema Generation

```
java -jar ./data/schema/avro/avro-tools-1.8.2.jar compile -string schema data/schema/avro social-network-analysis/target/generated-sources/avro
```

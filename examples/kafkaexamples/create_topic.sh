#!/bin/bash

docker-compose exec kafka kafka-topics --create --topic test_topic --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

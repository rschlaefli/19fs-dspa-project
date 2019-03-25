#!/bin/bash

docker-compose exec kafka kafka-topics --create --topic post --partitions 1 --replication-factor 1 --zookeeper 127.0.0.1:2181
docker-compose exec kafka kafka-topics --create --topic like --partitions 1 --replication-factor 1 --zookeeper 127.0.0.1:2181
docker-compose exec kafka kafka-topics --create --topic comment --partitions 1 --replication-factor 1 --zookeeper 127.0.0.1:2181

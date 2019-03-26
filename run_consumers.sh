#!/bin/sh

# TODO: fail if infrastructure is not running

docker-compose exec kafka kafka-topics.sh --create --topic post --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181
docker-compose exec kafka kafka-topics.sh --create --topic like --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181
docker-compose exec kafka kafka-topics.sh --create --topic comment --partitions 1 --replication-factor 1 --zookeeper zookeeper:2181

docker-compose up --build consumer_analysis

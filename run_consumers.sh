#!/bin/sh

# TODO: fail if infrastructure is not running

./run_topic_creation.sh

docker-compose up --build consumer_analysis

#!/bin/sh

scripts/run_topic_creation.sh

docker-compose up --build \
  --scale task-statistics=2 \
  --scale task-recommendations=2 \
  --scale task-anomalies=2 \
  cluster-statistics task-statistics \
  cluster-recommendations task-recommendations \
  cluster-anomalies task-anomalies

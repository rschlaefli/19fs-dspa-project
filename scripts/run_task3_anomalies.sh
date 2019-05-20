#!/bin/sh

docker-compose up --build --scale task-anomalies=2 cluster-anomalies task-anomalies

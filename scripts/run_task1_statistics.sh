#!/bin/sh

docker-compose up --build --scale task-statistics=2 cluster-statistics task-statistics

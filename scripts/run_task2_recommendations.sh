#!/bin/sh

docker-compose up --build --scale task-recommendations=2 cluster-recommendations task-recommendations

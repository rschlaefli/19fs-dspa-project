#!/bin/sh

TIMEOUT=180
QUIET=0

wait_for() {
  echo "Waiting for Kafka and Redis to be ready..." >&2

  for i in `seq $TIMEOUT` ; do
    nc -z "kafka" "9092" > /dev/null 2>&1
    result=$?

    nc -z "redis" "6379" > /dev/null 2>&1
    result=$(($result + $?))

    if [ $result -eq 0 ]; then
      if [ $# -gt 0 ] ; then
        exec "$@"
      fi
      exit 0
    fi
    sleep 1
  done
  echo "Operation timed out" >&2
  exit 1
}

wait_for "$@"

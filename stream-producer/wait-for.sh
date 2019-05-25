#!/bin/sh

TIMEOUT=180
QUIET=0

wait_for() {
  echo "Waiting for Kafka and Analytics Tasks ($1) to be ready..." >&2

  for i in `seq $TIMEOUT` ; do
    nc -z "kafka" "9092" > /dev/null 2>&1
    result=$?

    case "$1" in
      statistics)
        nc -z "cluster-statistics" "8081" > /dev/null 2>&1
        result=$(($result + $?))
        ;;

      recommendations)
        nc -z "cluster-recommendations" "8081" > /dev/null 2>&1
        result=$(($result + $?))
        ;;

      anomalies)
        nc -z "cluster-anomalies" "8081" > /dev/null 2>&1
        result=$(($result + $?))
        ;;

      *)
        nc -z "cluster-statistics" "8081" > /dev/null 2>&1
        result=$(($result + $?))
        nc -z "cluster-recommendations" "8081" > /dev/null 2>&1
        result=$(($result + $?))
        nc -z "cluster-anomalies" "8081" > /dev/null 2>&1
        result=$(($result + $?))
    esac

    if [ $result -eq 0 ]; then
      shift 1
      sleep 10
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

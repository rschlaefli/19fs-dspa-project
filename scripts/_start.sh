#!/bin/sh

# ensure that windows paths are converted
export COMPOSE_CONVERT_WINDOWS_PATHS=1

# set default parameters for task execution
TASK="all"
SERVICES="zookeeper kafka redis producer-post producer-comment producer-like"
export SOURCE_DIRECTORY="./"
export DATA_DIRECTORY="10k-users-sorted"
export MAXDELAYSEC="600"
export RANDOMDELAY="5"
export SPEEDUP="1500"
export TASK_PARALLELISM="1"
export STREAM_DIRECTORY="$SOURCE_DIRECTORY/data/$DATA_DIRECTORY/streams"

# extract the overall minimum timestamp from the datafiles
# this will allow to synchronize the producers to a common starting point
export PRODUCER_SYNC_TS=`python scripts/extract_min_timestamp.py --streamdir $STREAM_DIRECTORY`

for var in "$@"
do
  case "$1" in
    # if --build is specified, we need to rebuild all images to account for code changes
    "--build")
      docker-compose build --parallel cluster-statistics task-statistics producer-post web
      docker-compose build --parallel cluster-recommendations cluster-anomalies task-recommendations task-anomalies producer-comment producer-like
    ;;

    "--web")
      SERVICES="$SERVICES web"
    ;;

    "--speedup")
      SPEEDUP=$2
      shift 1
    ;;

    "--maxdelaysec")
      MAXDELAYSEC=$2
      shift 1
    ;;

    "--randomdelay")
      RANDOMDELAY=$2
      shift 1
    ;;

    "--parallelism")
      TASK_PARALLELISM=$2
      shift 1
    ;;

    "--source-dir")
      SOURCE_DIRECTORY=$2
      shift 1
    ;;

    "--data-dir")
      DATA_DIRECTORY=$2
      shift 1
    ;;

    "--task")
      case "$2" in
        statistics)
          TASK="statistics"
          SERVICES="--scale task-statistics=$TASK_PARALLELISM cluster-statistics task-statistics $SERVICES"
        ;;

        recommendations)
          TASK="recommendations"
          SERVICES="--scale task-recommendations=$TASK_PARALLELISM cluster-recommendations task-recommendations $SERVICES"
        ;;

        anomalies)
          TASK="anomalies"
          SERVICES="--scale task-anomalies=$TASK_PARALLELISM cluster-anomalies task-anomalies $SERVICES"
        ;;

        *)
          SERVICES="--scale task-statistics=$TASK_PARALLELISM --scale task-recommendations=$TASK_PARALLELISM --scale task-anomalies=$TASK_PARALLELISM \
            cluster-statistics task-statistics cluster-recommendations task-recommendations cluster-anomalies task-anomalies $SERVICES"
      esac
    ;;
  esac

  shift 1
done

# export the analytics task to allow for variable substitution in docker-compose
export TASK

# start all services including base infrastructure
docker-compose up --force-recreate $SERVICES

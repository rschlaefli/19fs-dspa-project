#!/bin/sh

# https://stackoverflow.com/questions/699576/validating-parameters-to-a-bash-script
die () {
    echo >&2 "$@"
    exit 1
}

# ensure that windows paths are converted
export COMPOSE_CONVERT_WINDOWS_PATHS=1

# set default parameters for task execution
BUILD=0
KAFKA_UI=0
VISUALIZATION_UI=1
TASK="all"
BASE_SERVICES="zookeeper kafka redis producer-post producer-comment producer-like"
FLINK_SERVICES=""
export SOURCE_DIRECTORY="./"
export DATA_DIRECTORY="10k-users-sorted"
export STREAM_DIRECTORY="${SOURCE_DIRECTORY}data/$DATA_DIRECTORY/streams"
export MAXDELAYSEC="600"
export SDELAY="600"
export SPEEDUP="3600"
export TASK_PARALLELISM="2"

# extract the overall minimum timestamp from the datafiles
# this will allow to synchronize the producers to a common starting point
export PRODUCER_SYNC_TS=`python scripts/extract_min_timestamp.py --streamdir $STREAM_DIRECTORY`
if [ ${#PRODUCER_SYNC_TS} = 0 ]; then
  echo "Timestamp extraction has failed. Please make sure that you are using Python 3 and that source/data dir are valid paths."
  exit 1
fi

for var in "$@"
do
  case "$1" in
    # if --build is specified, we need to rebuild all images to account for code changes
    "--build")
      BUILD=1
    ;;

    "--no-vis-ui")
      VISUALIZATION_UI=0
    ;;

    "--kafka-ui")
      KAFKA_UI=1
    ;;

    "--speedup")
      SPEEDUP=$2
      echo $SPEEDUP | grep -E -q '^[0-9]+$' || die "Invalid --speedup parameter $SPEEDUP! Please specify an integer >0."
    ;;

    "--maxdelaysec")
      MAXDELAYSEC=$2
      echo $MAXDELAYSEC | grep -E -q '^[0-9]+$' || die "Invalid --maxdelaysec parameter $MAXDELAYSEC! Please specify an integer >0."
    ;;

    "--schedulingdelay")
      SDELAY=$2
      echo $SDELAY | grep -E -q '^[0-9]+$' || die "Invalid --schedulingdelay parameter $SDELAY! Please specify an integer >0."
    ;;

    "--parallelism")
      TASK_PARALLELISM=$2
      echo $TASK_PARALLELISM | grep -E -q '^[0-9]+$' || die "Invalid --parallelism parameter $TASK_PARALLELISM! Please specify an integer >0."
    ;;

    "--source-dir")
      SOURCE_DIRECTORY=$2
      if [ ${#SOURCE_DIRECTORY} = 0 ]; then
        echo "Invalid --source-dir parameter! Please specify a valid directory path."
        exit 1
      fi
    ;;

    "--data-dir")
      DATA_DIRECTORY=$2
      if [ ${#DATA_DIRECTORY} = 0 ]; then
        echo "Invalid --data-dir parameter! Please specify a valid directory path."
        exit 1
      fi
    ;;

    "--person-ids")
      export PERSON_IDS=$2
      if [ ${#PERSON_IDS} = 0 ]; then
        echo "Invalid --person-ids parameter! Please specify a valid directory path."
        exit 1
      fi
    ;;

    "--task")
      case "$2" in
        statistics)
          TASK="statistics"
          FLINK_SERVICES="--scale task-statistics=$TASK_PARALLELISM cluster-statistics task-statistics $SERVICES"
        ;;

        recommendations)
          TASK="recommendations"
          FLINK_SERVICES="--scale task-recommendations=$TASK_PARALLELISM cluster-recommendations task-recommendations $SERVICES"
        ;;

        anomalies)
          TASK="anomalies"
          FLINK_SERVICES="--scale task-anomalies=$TASK_PARALLELISM cluster-anomalies task-anomalies $SERVICES"
        ;;

        recommendations+anomalies)
          TASK="recommendations"
          FLINK_SERVICES="--scale task-recommendations=$TASK_PARALLELISM \
                          --scale task-anomalies=$TASK_PARALLELISM \
                          cluster-recommendations task-recommendations \
                          cluster-anomalies task-anomalies"
        ;;

        *)
          echo "Invalid --task parameter! Use one of [statistics, recommendations, anomalies, recommendations+anomalies]. \
                Remove the parameter to run all tasks simultaneously."
          exit 1
      esac
    ;;
  esac

  shift 1
done

# export the analytics task to allow for variable substitution in docker-compose
export TASK

echo "Running application with the following parameters"
echo "--task=$TASK"
echo "--speedup=$SPEEDUP"
echo "--maxdelaysec=$MAXDELAYSEC"
echo "--schedulingdelay=$SDELAY"
echo "--parallelism=$TASK_PARALLELISM"
echo "--source-dir=$SOURCE_DIRECTORY"
echo "--data-dir=$DATA_DIRECTORY"
echo "--person-ids=$PERSON_IDS"
echo "Reading streams from $STREAM_DIRECTORY"
echo "Minimum synchronization timestamp $PRODUCER_SYNC_TS"

if [ $BUILD -eq 1 ]; then
  echo "Rebuilding images..."
  docker-compose build --parallel cluster-statistics task-statistics producer-post web
  docker-compose build --parallel cluster-recommendations cluster-anomalies task-recommendations task-anomalies producer-comment producer-like
fi

if [ ${#FLINK_SERVICES} -eq 0 ]; then
  FLINK_SERVICES="--scale task-statistics=$TASK_PARALLELISM \
                --scale task-recommendations=$TASK_PARALLELISM \
                --scale task-anomalies=$TASK_PARALLELISM \
                cluster-statistics task-statistics \
                cluster-recommendations task-recommendations \
                cluster-anomalies task-anomalies"
fi

if [ $KAFKA_UI -eq 1 ]; then
  BASE_SERVICES="$BASE_SERVICES kafka-web"
fi

if [ $VISUALIZATION_UI -eq 1 ]; then
  BASE_SERVICES="$BASE_SERVICES web"
fi

# start all services including base infrastructure
echo "Starting containers..."
docker-compose up --force-recreate $FLINK_SERVICES $BASE_SERVICES

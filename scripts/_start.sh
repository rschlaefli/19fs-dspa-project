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
TASK_OVERRIDE=""
BASE_SERVICES="zookeeper kafka redis producer-post producer-comment producer-like"
FLINK_SCALING=""
FLINK_SERVICES=""
SOURCE_DIRECTORY="./"
DATA_DIRECTORY="10k-users-sorted"
MAXDELAYSEC="600"
SDELAY="600"
SPEEDUP="1800"
TASK_PARALLELISM="2"
PERSON_IDS="4640 1597 9660 8054 6322 1327 6527 9696 9549 9900"
PERSON_IDS_CHANGED=0

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
      PERSON_IDS=`echo "$2" | tr -d '"'`
      PERSON_IDS_CHANGED=1
      if [ ${#PERSON_IDS} = 0 ]; then
        echo "Invalid --person-ids parameter! Please specify a valid list of person ids in quotes separated by spaces."
        exit 1
      fi
    ;;

    "--task")
      TASK_OVERRIDE="$2"
      case "$2" in
        statistics)
          PERSON_IDS_CHANGED=1
          TASK="statistics"
          FLINK_SERVICES="cluster-statistics task-statistics $SERVICES"
        ;;

        recommendations)
          TASK="recommendations"
          FLINK_SERVICES="cluster-recommendations task-recommendations $SERVICES"
        ;;

        anomalies)
          PERSON_IDS_CHANGED=1
          TASK="anomalies"
          FLINK_SERVICES="cluster-anomalies task-anomalies $SERVICES"
        ;;

        recommendations+anomalies)
          TASK="recommendations"
          FLINK_SERVICES="cluster-recommendations task-recommendations \
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

# extract the overall minimum timestamp from the datafiles
# this will allow to synchronize the producers to a common starting point
PRODUCER_SYNC_TS=`python scripts/extract_min_timestamp.py --streamdir ${SOURCE_DIRECTORY}data/$DATA_DIRECTORY/streams`
if [ ${#PRODUCER_SYNC_TS} = 0 ]; then
  echo "Timestamp extraction has failed. Please make sure that you are using Python 3 and that source/data dir are valid paths."
  exit 1
fi

echo "Running application with the following parameters"
echo "--task=$TASK_OVERRIDE"
echo "--speedup=$SPEEDUP"
echo "--maxdelaysec=$MAXDELAYSEC"
echo "--schedulingdelay=$SDELAY"
echo "--parallelism=$TASK_PARALLELISM"
echo "--source-dir=$SOURCE_DIRECTORY"
echo "--data-dir=$DATA_DIRECTORY"
echo "--person-ids=$PERSON_IDS"
echo "Reading streams from ${SOURCE_DIRECTORY}data/$DATA_DIRECTORY/streams"
echo "Minimum synchronization timestamp $PRODUCER_SYNC_TS"

if [ "$DATA_DIRECTORY" = "1k-users-sorted" ] && [ $PERSON_IDS_CHANGED -eq 0 ]; then
  die "Make sure to use person ids that are valid for 1k-users-sorted. We suggest using --person-ids \"294 166 344 740 724 722 273 345 658 225\"."
fi

export TASK
export SPEEDUP
export MAXDELAYSEC
export SDELAY
export TASK_PARALLELISM
export SOURCE_DIRECTORY
export DATA_DIRECTORY
export PERSON_IDS
export PRODUCER_SYNC_TS

if [ $BUILD -eq 1 ]; then
  echo "Rebuilding images..."
  docker-compose build --parallel cluster-statistics task-statistics producer-post web
  docker-compose build --parallel cluster-recommendations cluster-anomalies task-recommendations task-anomalies producer-comment producer-like
fi

case "$TASK_OVERRIDE" in
  statistics)
    FLINK_SCALE="--scale task-statistics=$TASK_PARALLELISM"
  ;;

  recommendations)
    FLINK_SCALE="--scale task-recommendations=$TASK_PARALLELISM"
  ;;

  anomalies)
    FLINK_SCALE="--scale task-anomalies=$TASK_PARALLELISM"
  ;;

  recommendations+anomalies)
    FLINK_SCALE="--scale task-recommendations=$TASK_PARALLELISM \
                 --scale task-anomalies=$TASK_PARALLELISM"
  ;;

  *)
    FLINK_SCALE="--scale task-statistics=$TASK_PARALLELISM \
                 --scale task-recommendations=$TASK_PARALLELISM \
                 --scale task-anomalies=$TASK_PARALLELISM"
esac

if [ ${#FLINK_SERVICES} -eq 0 ]; then
  FLINK_SERVICES="cluster-statistics task-statistics \
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
docker-compose up --force-recreate $FLINK_SCALE $FLINK_SERVICES $BASE_SERVICES

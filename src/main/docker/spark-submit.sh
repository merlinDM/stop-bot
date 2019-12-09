#!/bin/bash -e

if cp -r ./data/ /shared/ ; then
  echo "INFO: Moved test data to /shared/ dir"
else
  echo "WARN: Couldn't move test data to /shared/ dir"
fi

if mv -f ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_SHARED_JAR_LOCATION} ; then
  echo "INFO: Successfully moved $SPARK_APPLICATION_JAR_LOCATION to $SHARED_DIR"

  /spark/bin/spark-submit \
  --class ${SPARK_APPLICATION_MAIN_CLASS} \
  --master ${SPARK_MASTER_URL} \
  --deploy-mode client \
  --total-executor-cores 1 \
   ${SPARK_SUBMIT_ARGS} \
   ${SPARK_SHARED_JAR_LOCATION} \
   ${SPARK_APPLICATION_ARGS}
else
  ERROR_CODE=$?

  echo "ERROR: JAR NOT FOUND"

  exit ${ERROR_CODE}
fi
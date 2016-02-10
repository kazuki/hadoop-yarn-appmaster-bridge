#!/bin/bash
set -e
HDFS_HOME=hdfs:///user/${USER}
JAR_NAME=yarn-app-bridge-1.0.0.jar
JAR_HDFS_PATH=${HDFS_HOME}/${JAR_NAME}
PYPKG_NAME=yarn-app-sample-1.0.0.tar.gz
PYPKG_HDFS_PATH=${HDFS_HOME}/${PYPKG_NAME}
EXEC_NAME=exec_appmaster.sh
EXEC_HDFS_PATH=${HDFS_HOME}/${EXEC_NAME}
EXEC2_NAME=exec_executor.sh
EXEC2_HDFS_PATH=${HDFS_HOME}/${EXEC2_NAME}
python ./setup.py sdist
mvn package
hadoop fs -copyFromLocal -f ./${EXEC_NAME} ./${EXEC2_NAME} ./target/${JAR_NAME} ./dist/${PYPKG_NAME} ${HDFS_HOME}
hadoop jar ./target/${JAR_NAME} org.oikw.yarn_bridge.Client ${EXEC_HDFS_PATH} ${EXEC2_HDFS_PATH} @${PYPKG_HDFS_PATH} ${JAR_HDFS_PATH}

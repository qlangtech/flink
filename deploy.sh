#!/usr/bin/env bash

#mvn compile install -Dfast -Dscala-2.12 -DskipTests=true -DaltDeploymentRepository=base::default::http://localhost:8080/release
scala_version=2.11

mvn compile deploy -Pscala-${scala_version} -Dfast -DskipTests=true -Ptis-repo \
-pl flink-kubernetes\
,flink-table/flink-table-planner-blink\
,flink-table/flink-table-runtime-blink\
,flink-test-utils-parent/flink-test-utils\
,flink-test-utils-parent/flink-connector-test-utils\
,flink-core\
,flink-streaming-java\
,flink-table/flink-table-common\
,flink-tests\
,flink-runtime\
,flink-streaming-scala\
,flink-clients\
,flink-connectors/flink-connector-jdbc\
,flink-table/flink-table-api-java-bridge\
,flink-connectors/flink-connector-kafka\
,flink-connectors/flink-connector-rabbitmq\
,flink-connectors/flink-connector-elasticsearch7\
 -am

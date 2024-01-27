#!/usr/bin/env bash

#mvn compile install -Dfast -Dscala-2.12 -DskipTests=true -DaltDeploymentRepository=base::default::http://localhost:8080/release
scala_version=2.12

mvn compile deploy -Pscala-${scala_version} -Dfast  -DskipTests=true -Ptis-repo -Pdocs-and-source -Dcheckstyle.skip  \
-pl flink-dist,flink-kubernetes\
,flink-formats/flink-json\
,flink-table/flink-table-planner\
,flink-table/flink-table-runtime\
,flink-test-utils-parent/flink-test-utils\
,flink-test-utils-parent/flink-connector-test-utils\
,flink-core\
,flink-streaming-java\
,flink-table/flink-table-common\
,flink-tests\
,flink-runtime\
,flink-streaming-scala\
,flink-clients\
,flink-table/flink-table-api-java-bridge\
 -am


#,flink-connectors/flink-connector-jdbc\
#,flink-connectors/flink-connector-kafka\
#,flink-connectors/flink-connector-rabbitmq\
#,flink-connectors/flink-connector-elasticsearch7\

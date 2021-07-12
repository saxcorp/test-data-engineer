#!/bin/bash

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "LBC-AppExercise-CodingGame-{{ application.env }}" \
  --principal "{{ config.spark.kerberos.principal }}" \
  --keytab "{{ config.spark.kerberos.keytab.path }}" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --driver-memory "{{ config.spark.driver.memory }}" \
  --executor-memory "{{ config.spark.executor.memory }}" \
  --executor-cores "{{ config.spark.executor.cores }}"  \
  --files "{{ application.config-directory }}"/application.properties \
  --class com.littlebigcode.spark.TestDataEngineer.Main \
  lbc-test-data-engineer-*.jar \
  $@

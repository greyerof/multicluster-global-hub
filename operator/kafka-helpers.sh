#!/bin/bash

launch_kafka_consumer () {
  local consume=0
  if [ "$1" != "" ]; then
    consume=1
  fi

  local kafka_user_secret="global-hub-kafka-user"

  local kafka_bootstrap_server=`oc get kafka -n multicluster-global-hub kafka -ojsonpath='{.status.listeners[1].bootstrapServers}'`
  echo "Kafka BootStrap server: $kafka_bootstrap_server"

  oc get secret $kafka_user_secret -n multicluster-global-hub -o jsonpath='{.data.ca\.crt}' | base64 -d > ./ca.crt
  oc get secret $kafka_user_secret -n multicluster-global-hub -o jsonpath='{.data.user\.crt}' | base64 -d > ./client.crt
  oc get secret $kafka_user_secret -n multicluster-global-hub -o jsonpath='{.data.user\.key}' | base64 -d > ./client.key

  oc delete secret -n tests kafka-consumer-auth
  oc create secret -n tests generic kafka-consumer-auth --from-literal=bootstrap_server=$kafka_bootstrap_server --from-file=ca.crt=./ca.crt --from-file=client.crt=./client.crt --from-file=client.key=./client.key

  echo "Deploying kafka-consumer..."
  oc apply -f kafka-auto-consumer.yaml

  if [ "$consume" == "1" ]; then
    oc wait --for=condition=ContainersReady -n tests pod/kafka-consumer
    oc logs -n tests kafka-consumer --follow | tee kafka.log
  fi
}


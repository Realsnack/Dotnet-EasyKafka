#!/bin/bash

curl -XPOST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data "$(jq '{schema: tostring}' person.avsc | jq -r tostring)" 192.168.1.211:8081/subjects/person-topic-value/versions
#!/bin/bash
## Author: Joey Whelan
## Usage: run.sh
## Description:  Builds a 3-node Redis Enterpise cluster.  Loads the latest GA modules for search and json.  Builds
## a sharded db to house FHIR data.

SEARCH_LATEST=redisearch.Linux-ubuntu18.04-x86_64.2.6.5.zip
JSON_LATEST=rejson.Linux-ubuntu18.04-x86_64.2.4.3.zip

if [ ! -f $SEARCH_LATEST ]
then
    wget -q https://redismodules.s3.amazonaws.com/redisearch/$SEARCH_LATEST
fi 

if [ ! -f $JSON_LATEST ]
then
    wget https://redismodules.s3.amazonaws.com/rejson/$JSON_LATEST
fi 

echo "Launch Redis Enterprise docker containers"
docker compose up -d
echo "*** Wait for Redis Enterprise to come up ***"
curl -s -o /dev/null --retry 5 --retry-all-errors --retry-delay 3 -f -k -u "redis@redis.com:redis" https://localhost:19443/v1/bootstrap
echo "*** Build Cluster ***"
docker exec -it re1 /opt/redislabs/bin/rladmin cluster create name cluster.local username redis@redis.com password redis
docker exec -it re2 /opt/redislabs/bin/rladmin cluster join nodes 192.168.20.2 username redis@redis.com password redis
docker exec -it re3 /opt/redislabs/bin/rladmin cluster join nodes 192.168.20.2 username redis@redis.com password redis
echo "*** Load Modules ***"
curl -s -o /dev/null -k -u "redis@redis.com:redis" https://localhost:19443/v1/modules -F module=@$SEARCH_LATEST
curl -s -o /dev/null -k -u "redis@redis.com:redis" https://localhost:19443/v1/modules -F module=@$JSON_LATEST
echo "*** Build FHIR DB ***"
curl -s -o /dev/null -k -u "redis@redis.com:redis" https://localhost:19443/v1/bdbs -H "Content-Type:application/json" -d @fhirdb.json
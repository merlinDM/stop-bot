#!/bin/bash

docker run -d \
  --name cassandra-00 \
  --expose 9042:9042 \
  gd/cassandra:latest


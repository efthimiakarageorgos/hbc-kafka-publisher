#!/bin/bash

JARFILE="./build/libs/hbc-kafka-publisher-all.jar"

java -jar $JARFILE "$@"

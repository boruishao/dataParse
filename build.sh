#!/bin/bash

cd /d/program/dataParse
mvn package  -Dmaven.test.skip=true
#FinalDemo-1.0-SNAPSHOT.jar
scp target/dataParse-1.0-SNAPSHOT.jar root@com.patsnap.user1:/opt/model/hadoop-2.7.3/jars/mrTest.jar
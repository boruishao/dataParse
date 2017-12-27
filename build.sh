#!/bin/bash

cd /d/program/FinalDemo/
mvn package  -Dmaven.test.skip=true
#FinalDemo-1.0-SNAPSHOT.jar
scp /d/program/FinalDemo/target/FinalDemo-1.0-SNAPSHOT-shaded.jar root@192.168.111.129:/opt/model/spark-2.2.0-bin-hadoop2.7/jar/FinalDemo.jar
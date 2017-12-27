package com.patsnap.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SchemaDemo {

    public static void main(String[] args) {
        String filePath = args[0];
        String appName = "finalDemo";
        String master  = "local[3]";
        SparkSession spark =  SparkSession.builder().appName(appName).master(master).getOrCreate();
        Dataset<Row> json = spark.read().json(filePath);
        json.registerTempTable("temp1");
        Dataset<Row> temp1 = spark.sql("describe temp1");
//        temp1.write().json("temp2");
        json.write().saveAsTable("temp3");
        spark.stop();
    }
}

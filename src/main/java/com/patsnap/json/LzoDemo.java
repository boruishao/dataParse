package com.patsnap.json;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class LzoDemo {
    public static void main(String[] args) {
        String filePath = "src/main/resources/patent_classification_ipc-00000.lzo";
        String appName = "finalDemo";
        String master  = "local[3]";
        String id = "trademark_id";
        String tableName = "trademark";

        SparkSession spark =  SparkSession.builder().appName(appName).master(master).getOrCreate();
        SparkContext sc = spark.sparkContext();
//        RDD<Tuple2<LongWritable, Text>> lzoTupleRdd = (RDD<Tuple2<LongWritable, Text>>) sc.newAPIHadoopFile(filePath, LzoTextInputFormat.class, LongWritable.class, Text.class, new Configuration());

//        JavaRDD<String> lzoRdd = lzoTupleRdd.toJavaRDD().map(new Function<Tuple2<LongWritable, Text>, String>() {
//            @Override
//            public String call(Tuple2<LongWritable, Text> v1) throws Exception {
//                return v1._2.toString();
//            }
//        });
//
//        lzoRdd.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
    }
}

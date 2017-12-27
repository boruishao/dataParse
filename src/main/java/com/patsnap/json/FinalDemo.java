package com.patsnap.json;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.patsnap.utils.ParseRef;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FinalDemo {

    public static void main(String[] args) {
        if (args.length < 4) {
            throw new RuntimeException("至少输入前4个参数 1.源数据位置 2.表名/文件名 3.表主键名 4.输出路径（文件夹） 5.输出格式 6.appName 7.Master参数");
        }
        String filePath = args[0];
        String tableName = args[1];
        String id = args[2];
        String outPutPath = args[3];
        String fileType = null;
        if (!outPutPath.endsWith("/")) {
            outPutPath = outPutPath + "/";
        }
        final String LIST_TARGET = "list_target";
        SparkSession spark = null;
        if (args.length > 4) {
            fileType = args[4];
            String appName = args[5];
            String master = args[6];
            spark = SparkSession.builder().appName(appName).master(master).getOrCreate();
        } else {
            spark = SparkSession.builder().getOrCreate();
        }

        Dataset<String> textFile = spark.read().textFile(filePath).repartition(3);

        JavaRDD<Tuple2<String, String>> tupleRdd = textFile.javaRDD().flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                Map<String, String> normalJson = ParseRef.getNormalJsonByItem(s, tableName, id);
                List<Tuple2<String, String>> list = new ArrayList<>();
                for (Map.Entry<String, String> json : normalJson.entrySet()) {
                    list.add(new Tuple2<>(json.getKey(), json.getValue()));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<String, String> pairRDD = tupleRdd.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1, tuple2._2);
            }
        }).persist(new StorageLevel());

        JavaPairRDD<String, String> listRdd = pairRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                return v1._2.contains(LIST_TARGET);
            }
        });

        JavaPairRDD<String, String> mapRdd = pairRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                return !v1._2.contains(LIST_TARGET);
            }
        });

        JavaPairRDD<String, String> flatListRdd = listRdd.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                Map<String, Object> map = new Item().fromJSON(tuple._2).asMap();
                List<Tuple2<String, String>> list = new ArrayList<>();
                for (Object o : map.values()) {
                    if (o != null && o instanceof Map) {
                        list.add(new Tuple2<>(tuple._1, new Item().fromMap((Map<String, Object>) o).toJSON()));
                    }
                }
                return list.iterator();
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1, tuple2._2);
            }
        });


        JavaPairRDD<String, Iterable<String>> tablePairRDD = flatListRdd.union(mapRdd).groupByKey().persist(new StorageLevel());

        List<String> keys = tablePairRDD.keys().collect();

        for (String fileName : keys) {
            JavaRDD<String> tableRdd = tablePairRDD.filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Iterable<String>> v1) throws Exception {
                    return v1._1.equals(fileName);
                }
            }).values().flatMap(new FlatMapFunction<Iterable<String>, String>() {
                @Override
                public Iterator<String> call(Iterable<String> strings) throws Exception {
                    return strings.iterator();
                }
            });
            //默认写为json格式
            if (fileType != null && "csv".toLowerCase().equals(fileType)) {
                //写成csv文件
                spark.read().json(tableRdd).write().csv(outPutPath + fileName);
            } else {
                tableRdd.saveAsTextFile(outPutPath + fileName);
            }
        }
    }
}

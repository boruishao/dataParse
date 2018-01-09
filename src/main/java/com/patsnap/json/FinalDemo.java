package com.patsnap.json;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.patsnap.utils.ParseRef;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

public class FinalDemo {

    Logger logger = Logger.getLogger(this.getClass());

    public static void main(String[] args) {
        if (args.length < 4) {
            throw new RuntimeException("至少输入前4个参数 1.源数据位置 2.表名/文件名 3.表主键名 4.输出路径（文件夹）5.分区数 6.输出格式 7.appName 7.Master参数");
        }
        String filePath = args[0];
        String tableName = args[1];
        String id = args[2];
        String outPutPath = args[3];
        String partitionNum = args[4];
        String fileType = null;
        if (!outPutPath.endsWith("/")) {
            outPutPath = outPutPath + "/";
        }
        final String LIST_TARGET = "list_target";
        SparkSession spark = null;

        if (args.length > 5) {
            fileType = args[5];
            String appName = args[6];
            String master = args[7];
            spark = SparkSession.builder().appName(appName).master(master).getOrCreate();
        } else {
            spark = SparkSession.builder().getOrCreate();
        }
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        final LongAccumulator rightLines = spark.sparkContext().longAccumulator("rightLines");
        final LongAccumulator wrongLines = spark.sparkContext().longAccumulator("wrongLines");
        final LongAccumulator blankLines = spark.sparkContext().longAccumulator("blankLines");
        final LongAccumulator partAccumulator = spark.sparkContext().longAccumulator("partAccumulator");

        Dataset<String> textFile = spark.read().textFile(filePath).repartition(Integer.valueOf(partitionNum));

        JavaRDD<Tuple2<String, String>> tupleRdd = textFile.javaRDD().flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                Map<String, String> normalJson = ParseRef.getNormalJsonByItem(s, tableName, id);
                if (s.trim().equals("")) {
                    blankLines.add(1);
                } else if (normalJson.size() == 0) {
                    wrongLines.add(1);
                } else {
                    rightLines.add(1);
                }
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
        });

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

        JavaPairRDD<String, String> unionJavaPairRDD = flatListRdd.union(mapRdd);
        //获取分区数
        int numPartitions = unionJavaPairRDD.getNumPartitions();

        //通过随机前缀重分区
        JavaPairRDD<String, String> partitionRdd = unionJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> v1) throws Exception {
                partAccumulator.add(1);
                return new Tuple2(partAccumulator.value() % numPartitions + "," + v1._1, v1._2);
            }
        });

        /*************************************************new Method*********************************************************************/

//        JavaPairRDD<String, String> tablePairRDD2 = partitionRdd.persist(StorageLevel.MEMORY_AND_DISK());
//
//        List<String> distinctkeys = tablePairRDD2.keys().distinct().collect();
//        Set<String> keysSet = new LinkedHashSet<>();
//        for (String  key  : distinctkeys) {
//            keysSet.add(key.substring(key.indexOf(",")+1));
//        }
//
//        Broadcast<String[]> fileNames = sc.broadcast(keysSet.toArray(new String[0]));
//
//        for (String fileName : fileNames.value()) {
//            JavaRDD<String> tableRdd2 = tablePairRDD2.filter(new Function<Tuple2<String, String>, Boolean>() {
//                @Override
//                public Boolean call(Tuple2<String, String> v1) throws Exception {
//                    return v1._1.substring(v1._1.indexOf(",")+1).equals(fileName);
//                }
//            })
//            .values();
//            //默认写为json格式
//            if (fileType != null && "csv".toLowerCase().equals(fileType)) {
//                //写成csv文件
//                spark.read().json(tableRdd2).write().csv(outPutPath + fileName);
//            } else {
//                tableRdd2.saveAsTextFile(outPutPath + fileName);
//            }
//        }
        /**********************************************oldMethod************************************************************************/
        JavaPairRDD<String, Iterable<String>> tablePairRDD = partitionRdd.groupByKey().persist(StorageLevel.MEMORY_AND_DISK());

        List<String> keysList = tablePairRDD.keys().collect();
        Set<String> keysSet = new LinkedHashSet<>();
        for (String key : keysList) {
            keysSet.add(key.substring(key.indexOf(",") + 1));
        }

        Broadcast<String[]> fileNames = sc.broadcast(keysSet.toArray(new String[0]));

        for (String fileName : fileNames.value()) {
            JavaRDD<String> tableRdd = tablePairRDD.filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Iterable<String>> v1) throws Exception {
                    return v1._1.substring(v1._1.indexOf(",") + 1).equals(fileName);
                }
            })
                    .values()
                    .flatMap(new FlatMapFunction<Iterable<String>, String>() {
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

        System.out.println("successful rows : " + rightLines.value());
        System.out.println("wrong rows : " + wrongLines.value());
        System.out.println("blank rows : " + blankLines.value());
        spark.close();
    }
}

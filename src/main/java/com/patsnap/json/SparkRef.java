package com.patsnap.json;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.patsnap.utils.ParseRef;
import net.sf.json.JSONArray;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkRef {
    public static void main(String[] args) {

        String filePath = "src/main/resources/trademark_biblio.json";
        String appName = "finalDemo";
        String master  = "local[3]";
        String id = "trademark_id";
        String tableName = "trademark";

        SparkSession spark =  SparkSession.builder().appName(appName).master(master).getOrCreate();
        Dataset<String> textFile = spark.read().textFile(filePath);

        BufferedWriter fields =null;

        JavaRDD<Tuple2<String, String>> tupleRdd = textFile.javaRDD().flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                Map<String, String> normalJson = ParseRef.getNormalJsonByItem(s,tableName,id);
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
                return v1._2.contains("list_target");
            }
        });

        JavaPairRDD<String, String> mapRdd = pairRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                return !v1._2.contains("list_target");
            }
        });

        //test
        JavaPairRDD<String, String> flatListRdd = listRdd.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                Map<String,Object> map = new Item().fromJSON(tuple._2).asMap();
                List<Tuple2<String, String>> list = new ArrayList<>();
                for (Object o :map.values()) {
                    if(o!=null && o instanceof Map){
                        list.add(new Tuple2<>(tuple._1,new Item().fromMap((Map<String, Object>) o).toJSON()));
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

        //old
        JavaPairRDD<String, String> flatListRdd2 = listRdd.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                JSONArray jsonArray = JSONArray.fromObject(tuple._2);
                List<Tuple2<String, String>> list = new ArrayList<>();
                for (Object obj : jsonArray) {
                    list.add(new Tuple2<>(tuple._1, obj.toString()));
                }
                return list.iterator();
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<>(tuple2._1, tuple2._2);
            }
        });

        JavaPairRDD<String, Iterable<String>> tablePairRDD = flatListRdd.union(mapRdd).groupByKey();

        tablePairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
//                BufferedWriter bw = new BufferedWriter(new FileWriter("./file/"+tableName+((tableName.equals(tuple._1))?"":("_"+tuple._1))));
                BufferedWriter bw = new BufferedWriter(new FileWriter("./file/" + tuple._1));
                for (String str : tuple._2){
                    bw.write(str);
                    bw.newLine();
                }
                bw.close();
            }
        });

    }
}

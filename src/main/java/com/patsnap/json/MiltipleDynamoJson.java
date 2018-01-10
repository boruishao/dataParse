package com.patsnap.json;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.patsnap.utils.ParseRef;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MiltipleDynamoJson {
    Logger logger = Logger.getLogger(this.getClass());

    public static void main(String[] args) {

        String filePath = "src/main/resources/sample.json";

        String id = "trademark_id";
        String tableName = "trademark";


        String outPutPath = ".";

        String fileType = null;
        if (!outPutPath.endsWith("/")) {
            outPutPath = outPutPath + "/";
        }
        final String LIST_TARGET = "list_target";
        SparkSession spark = null;
        spark = SparkSession.builder().master("local[2]").getOrCreate();

        final LongAccumulator rightLines = spark.sparkContext().longAccumulator("rightLines");
        final LongAccumulator wrongLines = spark.sparkContext().longAccumulator("wrongLines");
        final LongAccumulator blankLines = spark.sparkContext().longAccumulator("blankLines");

        Dataset<String> textFile = spark.read().textFile(filePath);

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

        JavaRDD<Row> rowRdd = unionJavaPairRDD.map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2);
            }
        });

        StructField[] sfs ={new StructField("fileName", DataTypes.StringType,true, Metadata.empty()),
                new StructField("record", DataTypes.StringType,true, Metadata.empty())};

       Dataset<Row> unionDf = spark.createDataFrame(rowRdd, new StructType(sfs));
        /*****************************************************************************************************************/

//        Dataset<Row> unionDf = spark.createDataFrame(javaRDD, PairBean.class);
//
//        try {
//            unionDf.write().partitionBy(PairBean.class.getDeclaredField("tableName").getName()).format("text").parquet(outPutPath);
//        } catch (NoSuchFieldException e) {
//            e.printStackTrace();
//        }
        unionDf.show(1);
        unionDf.registerTempTable("test");
        Dataset<Row> sql = spark.sql("select * from test");
        sql.show(2);


        /*****************************************************************************************************************/


        System.out.println("successful rows : " + rightLines.value());
        System.out.println("wrong rows : " + wrongLines.value());
        System.out.println("blank rows : " + blankLines.value());
        spark.close();
    }
}

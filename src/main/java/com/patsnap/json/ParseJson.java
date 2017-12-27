package com.patsnap.json;

import com.patsnap.utils.ParseJsonUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ParseJson {
    public static void main(String[] args) {
//        String filePath = args[0];
        String filePath = "src/main/resources/sample.json";
        String appName = "finalDemo";
        String master  = "local[3]";
        BufferedWriter fields =null;

        if (args.length>2){
            appName=args[1];
            master=args[2];
        }
        SparkSession spark =  SparkSession.builder().appName(appName).master(master).getOrCreate();

        Dataset<String> textFile = spark.read().textFile(filePath);
        JavaRDD<String> normalJson = textFile.javaRDD().map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
//                return ParseJsonUtils.horizontalParseJson(s);
                String s1  = ParseJsonUtils.longitudinalParseJson(s);
                return s1;
            }
        });

//        normalJson.collect().forEach(new Consumer<String>() {
//            @Override
//            public void accept(String s) {
//                System.out.println(s);

//            }
//        });
        //获取schema
        Set<String> reduce = normalJson.map(new Function<String, Set<String>>() {
            @Override
            public Set<String> call(String v1) throws Exception {
                Set entrySet = JSONObject.fromObject(v1).entrySet();

                return JSONObject.fromObject(v1).keySet();
            }
        }).reduce(new Function2<Set<String>, Set<String>, Set<String>>() {
            @Override
            public Set<String> call(Set<String> v1, Set<String> v2) throws Exception {
                Set<String> v3 = new HashSet<>();
                v3.addAll(v1);
                v3.addAll(v2);
                return v3;
            }
        });

        //schema写入文件
        try {
            fields = new BufferedWriter(new FileWriter("fields"));
            for (String line : reduce) {
                fields.write(line);
                fields.newLine();
            }
            fields.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fields.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //后续运算
        Dataset<Row> jsonDs = spark.read().json(normalJson);
        jsonDs.createOrReplaceGlobalTempView("temp1");//global_temp
        Dataset<Row> field = spark.sql("SELECT count(1),create_ts,avg(update_ts),sum(andt),max(trademark_source_id) FROM global_temp.temp1 group by andt,create_ts order by 1 limit 15");
        field.rdd();

        field.show();
        spark.stop();
    }
}

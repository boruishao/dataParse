package com.patsnap.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ReadJson {
     public static void main(String[] args) throws IOException {
         FileSystem hdfs = null;
         List<Path> files = null;
         try {
             Configuration config = new Configuration();
             // 程序配置
             config.set("fs.default.name", "hdfs://com.patsnap.user-1:8020");
             hdfs = FileSystem.get(new URI("hdfs://com.patsnap.user-1:8020"),
                     config, "root");
             Path path = new Path("/user/root/parseJson/output2");
             files = showFiles(hdfs, path);

         } catch (Exception e) {
             e.printStackTrace();
         } finally {
             if (hdfs != null) {
                 try {
                     hdfs.closeAll();
                 } catch (Exception e) {
                     e.printStackTrace();
                 }
             }
         }
         SparkConf conf = new SparkConf();
         SparkSession spark = SparkSession.builder()
                 .config("-XX","-UseConcMarkSweepGC")
                 .appName("readJson")
                 .master("spark://com.patsnap.user-1:7077")
                 .config("total.executor.cores","1")
                 .config("spark.executor.memory","1G")
                 .getOrCreate();

         for (Path path : files) {
             Dataset<Row> json = spark.read().json(path.toString());
             json.write().mode(SaveMode.Append).csv("/user/root/parseJson/output2_json/"+path.getName());
//                     .json("/user/root/parseJson/output2_json/"+path.getName());
             json.printSchema();
         }
        spark.stop();
 }

    public static List<Path> showFiles(FileSystem hdfs ,Path path){
        List<Path> list = new ArrayList<>();
        try {
            if(hdfs == null || path == null){
                return null;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            for (FileStatus fsu : files) {
               list.add(fsu.getPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void iteratorShowFiles(FileSystem hdfs, Path path){
        try{
            if(hdfs == null || path == null){
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try{
                    if(files[i].isDirectory()){
                        System.out.println(">>>" + files[i].getPath()
                                + ", dir owner:" + files[i].getOwner());
                        //递归调用
                        iteratorShowFiles(hdfs, files[i].getPath());
                    }else if(files[i].isFile()){
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

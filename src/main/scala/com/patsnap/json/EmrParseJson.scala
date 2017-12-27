package com.patsnap.json

import com.patsnap.utils.SparkJsonUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EmrParseJson {
  def main(args: Array[String]): Unit = {
//    val master = args(0)
//    val filePath = args(0)
    val filePath = "s3://patsnap-bucket/data/input/sample.json"
    val conf = new SparkConf().setAppName("parse").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile: RDD[String] = sc.textFile(filePath)
    val normalJsonRdd = textFile.map(SparkJsonUtils.longitudinalParseJson(_)).persist()
    normalJsonRdd.saveAsTextFile("s3://patsnap-bucket/data/output")
    sc.stop()
  }
}

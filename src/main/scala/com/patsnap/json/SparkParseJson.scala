package com.patsnap.json

import java.io.{BufferedWriter, FileWriter, IOException}

import com.patsnap.utils.SparkJsonUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


object SparkParseJson {

  case class Ncl(code: String, rowid: String, uuid: String)

  def main(args: Array[String]): Unit = {
    //    val filePath = args(0)
    val filePath = "src/main/resources/sample.json"
    var appName = "parseJson"
    var master = "local[2]"
    if (args.length > 2) {
      appName = args(1)
      master = args(2)
    }
    val spark = SparkSession.builder().master(master).appName(appName).getOrCreate()

    val file = spark.read.textFile(filePath)
    val normalJson = file.rdd.map((line: String) => {
      SparkJsonUtils.longitudinalParseJson(line)
    })

    val assignee = normalJson.map(SparkJsonUtils.getList("assignee",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("assignee")
    val correspondence = normalJson.map(SparkJsonUtils.getList("assignee",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("correspondence")
    val localNcl = normalJson.map(SparkJsonUtils.getList("assignee",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("localNcl")
    val ncl = normalJson.map(SparkJsonUtils.getList("ncl",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("ncl")
    val representative = normalJson.map(SparkJsonUtils.getList("representative",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("representative")
    val uscl = normalJson.map(SparkJsonUtils.getList("uscl",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("uscl")
    val vcl = normalJson.map(SparkJsonUtils.getList("vcl",_)).flatMap(SparkJsonUtils.getMapInList(_)).setName("vcl")

    val trademark_source_id = normalJson.map(SparkJsonUtils.getMap("trademark_source_id",_)).setName("trademark_source_id")

    val rddjson = normalJson.map(SparkJsonUtils.list2uuid(_)).map(SparkJsonUtils.map2uuid(_)).setName("rddJson")

    val buffer = ArrayBuffer(assignee,correspondence,localNcl,ncl,representative,uscl,vcl,trademark_source_id,rddjson)
    for (ele  <- buffer){
      val strings = ele.collect()
      val writer = new BufferedWriter(new FileWriter("./file/" + ele.name))
      try {
        for (str <- strings) {
          writer.write(str)
          writer.newLine()
        }
      } catch {
        case e: IOException => e.printStackTrace()
        case _ => println("=================")
      } finally {
        writer.close()
      }
    }
    spark.stop()
  }


}

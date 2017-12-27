//package com.patsnap.helper
//
//import org.apache.spark.{SparkConf, SparkContext}
//
//object SparkParser {
//    def main(args: Array[String]) {
//        if (args.length<2){
//            System.err.println("Usage: SparkGrep <host> <input_file>")
//            System.exit(1)
//        }
//
//        val conf = new SparkConf().setAppName("SparkParser").setMaster(args(0))
//        val sc = new SparkContext(conf)
//        val inputFile = sc.textFile(args(1),2).cache()
//        println("%s lines in the file %s".format(inputFile.count(),args(1)))
//
//        val linesRdd=inputFile.map(line => {normalize(line)});
//
//
//        linesRdd.foreach{println}
//        System.exit(0)
//    }
//}

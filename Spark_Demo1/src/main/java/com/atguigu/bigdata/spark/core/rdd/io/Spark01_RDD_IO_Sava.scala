package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Sava {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

   val rdd = sc.makeRDD(
     List(
       ("a", 1),
       ("b", 2),
       ("c", 3),
       ("d", 4),
     )
   )
    // 数据的存储
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    // 数据的读取
    val inputRDD: RDD[String] = sc.textFile("output1")
    sc.objectFile[Int]("output3").collect().foreach(println)
    sc.sequenceFile[Int, Int]("output3").collect().foreach(println)

    sc.stop()
  }

}

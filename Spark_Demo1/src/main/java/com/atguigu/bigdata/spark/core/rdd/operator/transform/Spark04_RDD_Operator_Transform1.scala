package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - flatMap
    val rdd: RDD[String] = sc.makeRDD(List(
      "Hello Scala", "Hello Spark"
    ))

    val fmRDD: RDD[String] = rdd.flatMap(
      s => {
        val datas = s.split(" ")
        datas
      }
    )

    val fmRDD1: RDD[String] = rdd.flatMap(
      s => {
        val datas = s.split(" ")
        datas
      }
    )
    fmRDD.collect().foreach(println)

    sc.stop()
  }
}

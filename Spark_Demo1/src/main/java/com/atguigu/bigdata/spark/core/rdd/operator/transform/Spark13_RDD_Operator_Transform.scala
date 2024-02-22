package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - intersection 求交集
    val dataRDD1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val dataRDD2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

    val dataRDD: RDD[Int] = dataRDD1.intersection(dataRDD2)
    dataRDD.collect().foreach(println)

  }

}

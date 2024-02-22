package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - distinct

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 1, 4, 5, 2))

    val distinctRDD: RDD[Int] = rdd.distinct()

    distinctRDD.collect().foreach(println)
     sc.stop()
  }

}

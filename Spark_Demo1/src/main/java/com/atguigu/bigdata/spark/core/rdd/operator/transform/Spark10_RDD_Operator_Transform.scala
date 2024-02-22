package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - coalesce 根据数据量缩减分区 默认是不进行shuffle

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 6)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)
    val coalesceRDD1: RDD[Int] = rdd.coalesce(2, true)

    coalesceRDD1.saveAsTextFile("output")
     sc.stop()
  }

}

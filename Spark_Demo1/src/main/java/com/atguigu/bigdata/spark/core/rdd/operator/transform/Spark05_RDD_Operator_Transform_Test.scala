package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - glom 将每个分区的所有元素，封装到一个Array中，再将Array放入RDD
    // 分区内取最大值, 分区间求和
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))
    sc.stop()
  }
}

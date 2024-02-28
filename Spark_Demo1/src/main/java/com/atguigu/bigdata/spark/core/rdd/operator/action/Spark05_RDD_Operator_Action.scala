package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))

    // TODO 行动算子 countByValue
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()

    println(intToLong)

    // TODO 行动算子 countByKey
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1)))

    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()

    println(stringToLong)
    sc.stop()
  }
}

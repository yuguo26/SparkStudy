package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - sortBy
    val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt)
    newRDD.collect().foreach(println)
  }

}

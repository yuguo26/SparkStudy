package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))

    // TODO 行动算子 reduce
    // aggregateByKey 只会参与分区内计算
    // aggregate 不仅仅参与分区内计算, 并且参与分区间计算
    val result: Int = rdd.aggregate(0)(_ + _, _ + _)

    println(result)
    sc.stop()
  }
}

package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))

    // TODO 行动算子 fold
    // aggregateByKey 只会参与分区内计算
    // aggregate 不仅仅参与分区内计算, 并且参与分区间计算

    // 当分区内和分区间计算规则相同时, 就可以用fold
    val result: Int = rdd.fold(0)(_ + _)

    println(result)
    sc.stop()
  }
}

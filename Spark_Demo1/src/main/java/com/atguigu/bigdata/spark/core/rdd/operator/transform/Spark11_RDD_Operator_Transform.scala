package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - repartition 内部其实执行的是coalesce 操作, 参数shuffle的默认值是true
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val repartitionRDD: RDD[Int] = rdd.repartition(4)

    repartitionRDD.saveAsTextFile("output")
     sc.stop()
  }

}

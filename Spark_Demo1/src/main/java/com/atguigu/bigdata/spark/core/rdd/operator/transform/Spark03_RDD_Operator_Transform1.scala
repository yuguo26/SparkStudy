package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1,2,3,4))

    // 获取每个分区的最大值
    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
          // (0, 1),(2, 2),(4, 3),(6, 4)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpRDD.collect().foreach(println)
    sc.stop()
  }

}

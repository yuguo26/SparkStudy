package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - sample 从数据集中抽取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // sample 算子需要传递三个参数
    // 1. 第一个参数: 抽取的数据是否放回, false不放回
    // 2. 第二个参数: 抽取的几率, 范围在[0, 1]之间,0: 全不取, 1: 全取
    // 3. 第三个参数: 随机数种子
    val sampleRDD: RDD[Int] = rdd.sample(
      withReplacement = false,
      fraction = 0.4,
      seed = 1
    )

    sampleRDD.collect().mkString(",")foreach(println)
     sc.stop()
  }

}

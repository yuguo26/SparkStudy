package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - Key-Value类型 foldByKey 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 4), ("b", 5)), 2)

    // aggregateByKey 存在函数柯里化, 有两个参数列表
    // 第一个参数列表, 需要传递一个参数表示初始值
    // 第二个参数列表需要传递两个参数, 第一个参数表示分区内的计算规则, 第二个参数表示分区间的计算规则
    //rdd.saveAsTextFile("output1")

    // 分区内核分区间计算规则相同
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop()
  }

}

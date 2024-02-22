package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - Key-Value类型 aggregateByKey 将数据根据不同的规则进行分区内的计算和分区间的计算
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 4), ("b", 5)), 2)

    // aggregateByKey 存在函数柯里化, 有两个参数列表
    // 第一个参数列表, 需要传递一个参数表示初始值
    // 第二个参数列表需要传递两个参数, 第一个参数表示分区内的计算规则, 第二个参数表示分区间的计算规则
    //rdd.saveAsTextFile("output1")
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    // 分区内核分区间计算规则相同
    rdd.aggregateByKey(0)(_+_, _+_).collect().foreach(println)

    sc.stop()
  }

}

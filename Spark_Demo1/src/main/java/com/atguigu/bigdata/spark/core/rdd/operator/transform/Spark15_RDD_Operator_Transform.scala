package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - Key-Value类型 ReduceByKey
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // reduceByKey: 想通的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合, spark是基于scala开发的, 所以spark也是两两聚合
    // reduceByKey 中，如果key的值只有一个那么这个值是不会参与计算的
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = ${x}, y = ${y}")
      x + y
    })

    reduceByKeyRDD.collect().foreach(println)
  }

}

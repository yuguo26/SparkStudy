package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))

    // TODO 行动算子 reduce
    // 行动算子就是触发作业的执行
    val reduce: Int = rdd.reduce(_ + _)

    // collect 方法会将不同分区的数据按照分区的顺序采集到Driver端内存中,形成数组
    val ints: Array[Int] = rdd.collect()

    // count: 数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    // first: 获取数据源中第一个数据
    val fst: Int = rdd.first()
    println(fst)

    // take: 取前三个数据
    val ints1: Array[Int] = rdd.take(3)
    println(ints1.mkString(","))

    // takeOrdered 数据排序后, 取N个数据 默认升序
    val takeOrdered: Array[Int] = rdd.takeOrdered(3)
    println(takeOrdered.mkString(","))

    // println(ints.mkString(","))
    sc.stop()
  }
}

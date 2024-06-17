package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    // Spark 封装了三大数据结构,
    // --> RDD: 弹性分布式数据集
    // --> 累加器: 分布式共享只写变量
    // --> 广播变量: 分布式共享只读变量
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // reduce: 包括分区内的计算和分区间的计算
    //val i: Int = rdd.reduce(_ + _)
    //println(i)

    var sum = 0

     //foreach 没有分区内和分区间的概念， 由于spark闭包，数据在executor计算完之后无法返回给driver, 这个时候就需要acc了
    rdd.foreach(
      num => {
        sum += num
      }
    )

    println("sum = " + sum)
    sc.stop()
  }

}

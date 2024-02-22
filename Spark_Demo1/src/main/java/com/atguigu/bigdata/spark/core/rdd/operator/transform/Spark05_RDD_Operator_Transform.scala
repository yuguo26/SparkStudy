package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - glom 将每个分区的所有元素，封装到一个Array中，再将Array放入RDD
    // 所有分区最大值求和
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )

    val sum: Int = maxRDD.collect().sum
    println(sum)

    // 网上看到的简便方法
    //println(rdd.glom().map(datas => datas.max).sum())
    sc.stop()
  }

}

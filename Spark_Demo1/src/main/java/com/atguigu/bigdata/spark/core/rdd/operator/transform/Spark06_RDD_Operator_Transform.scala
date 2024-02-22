package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - groupBy 将数据根据指定的规则进行分组, 分区默认不变, 但是数据会被打乱重新组合, 我们将这样的操作称之为shuffle. 极限情况下, 数据可能被分在同一个分区中
    // 一个组的数据在一个分区中, 但是并不是说一个分区中只有一个组
    // 所有分区最大值求和
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    def groupFunction(num:Int): Int = {
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    groupRDD.collect().foreach(println)

    sc.stop()
  }

}

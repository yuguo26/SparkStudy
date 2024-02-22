package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter 根据指定规则进行筛选过滤, 符合规则的数据保留, 不符合规则的数据丢弃
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    def filterFunction(num:Int):Boolean = {
      num % 2 != 0
    }

    val filterRDD: RDD[Int] = rdd.filter(filterFunction)
    filterRDD.collect().foreach(println)

    sc.stop()
  }

}

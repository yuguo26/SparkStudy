package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - key-value leftOuterJoin 左外连接, 同理有rightOuterJoin
    // 如果两个数据源的key没有匹配上, 那么数据不会出现在结果中
    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 4), ("b", 5) , ("f", 5)) )
    val rdd2 = sc.makeRDD(List(("a", 9), ("a", 12), ("a", 13), ("a", 4), ("b", 4), ("b", 15), ("c", 15)) )

    val leftOuterJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    leftOuterJoinRDD.collect().foreach(println)
    sc.stop()
  }

}

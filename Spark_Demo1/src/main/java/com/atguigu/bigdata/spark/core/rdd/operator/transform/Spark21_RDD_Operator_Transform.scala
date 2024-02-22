package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - key-value join 在类型为(K,V)和(K,w) 的RDD上调用, 返回一个相同key对应的所有元素连接在一起
    // 如果两个数据源的key没有匹配上, 那么数据不会出现在结果中
    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 4), ("b", 5)), 2)
    val rdd2 = sc.makeRDD(List(("a", 9), ("a", 12), ("a", 13), ("a", 4), ("b", 4), ("b", 15), ("c", 15)), 2)

    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)
    sc.stop()
  }

}

package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 行动算子 save
    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))

    // 1. saveAsTextFile 保存为Text文件
    rdd.saveAsTextFile("output")

    // 2. saveAsObjectFile 序列化对象保存到不同的文件中
    rdd.saveAsObjectFile("output1")

    // 3. saveAsSequenceFile 保存成Sequencefile文件, 方法要求数据的格式为k-v类型
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("a", 1)))
    rdd1.saveAsSequenceFile("output3")

    sc.stop()
  }
}

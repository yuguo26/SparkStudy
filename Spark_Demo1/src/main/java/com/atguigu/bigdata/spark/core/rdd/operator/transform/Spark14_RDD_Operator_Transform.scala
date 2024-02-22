package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

object Spark14_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - Key-Value 类型 PartitionBy 数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
    val rdd = sc.makeRDD(List(1,2,3,4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    //rdd.partitionBy

  }

}

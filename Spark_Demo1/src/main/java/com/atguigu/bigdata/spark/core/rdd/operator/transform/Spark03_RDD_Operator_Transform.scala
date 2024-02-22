package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex 将待处理的数据以分区为单位发送到计算节点进行处理
    val rdd = sc.makeRDD(List(1,2,3,4), 2)

    // 获取第二个分区的数据
    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
          if(index == 1){
            iter
          }else {
            Nil.iterator
          }
      }
    )

    mpRDD.collect().foreach(println)
    sc.stop()
  }

}

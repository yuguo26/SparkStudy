package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions 将待处理的数据以分区为单位发送到计算节点进行处理
    val rdd = sc.makeRDD(List(1,2,3,4), 2)

    // mapPartitions 可以以分区为单位进行数据转换操作
    //        但是会将整个分区的数据加载进内存进行引用
    //        如果处理完的数据是不会被释放掉, 存在对象的引用
    //        在内存较小数据量较大的情况下, 容易出现内存泄漏
    val mpRDD:RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>")
        iter.map(_ * 2)
      }
    )

    mpRDD.collect().foreach(println)
    sc.stop()
  }

}

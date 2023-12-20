package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 创建RDD
    // 从文件中创建RDD, 将文件的数据作为处理的数据源

    // path路径默认是当前环境的根路径为基准. 可以写绝对路径, 也可以写相对路径
   //  sc.textFile("E:\\School\\project\\SparkStudy\\datas\\1.txt")
   //  val rdd:RDD[String] = sc.textFile("datas/1.txt")

    // 特殊一些，对路径下所有文件进行统计
    //  val rdd:RDD[String] = sc.textFile("datas")
      val rdd:RDD[String] = sc.textFile("datas/1*.txt")

    // path还可以是hdfs路径


    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}

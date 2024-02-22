package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 案例实操
    // 1. 获取原始数据 时间戳, 省份, 城市, 用户, 广告
    val sourceRDD: RDD[String] = sc.textFile("E:\\School\\project\\SparkStudy\\datas\\agent.log")

    // 2. 将原始数据进行结构转换


  }
}

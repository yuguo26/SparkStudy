package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark07_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter 根据指定规则进行筛选过滤, 符合规则的数据保留, 不符合规则的数据丢弃
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val mapRDD: RDD[(String, String)] = rdd.map(
      line => {
        val datas = line.split(" ")
        val times = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(times)
        val sdf1 = new SimpleDateFormat("yyyyMMdd")
        val hour: String = sdf1.format(date)

        val path = datas(6)
        (hour, path)
      }
    )

    mapRDD.collect().foreach(println)


    val filterRDD: RDD[(String, String)] = mapRDD.filter(
      line => {
        line._1 == 20150517
      }
    )

    val filterRDD1: RDD[String] = rdd.filter(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        time.startsWith("17/05/2015")
      }
    )

    filterRDD.collect().foreach(println)
    println("--------------->方法分割--------------->")
    filterRDD1.collect().foreach(println)
    sc.stop()
  }

}

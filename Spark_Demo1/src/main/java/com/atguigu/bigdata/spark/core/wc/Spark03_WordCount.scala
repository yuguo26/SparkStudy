package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {



    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    // spark可以将 分组和聚合融合成一个方法实现
    // reduceByKey: 相同的key的数据, 可以对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)
    //  wordToOne.reduceByKey((x, y) => {x + y})
    //  wordToOne.reduceByKey((x, y) => x + y)
    //  wordToOne.reduceByKey(_ + _)

    //  5.将转换的结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
  }
}

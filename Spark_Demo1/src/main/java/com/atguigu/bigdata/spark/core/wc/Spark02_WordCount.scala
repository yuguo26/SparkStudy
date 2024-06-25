package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    // Application
    // Spark框架
    // TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    // TODO 执行业务操作

    // 1.读取文件, 获取一行一行的数据
    //    1.hello world
    val lines: RDD[String] = sc.textFile("datas")

    // 2.将一行数据进行拆分, 形成一个一个的单词
    //    扁平化, 将整体拆分成个体的操作
    //    "hello world" => hello, world, hello, world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )

    //  4.对分组后的数据进行聚合
    //     (hello, 3), (world, 2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    //  5.将转换的结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }
}

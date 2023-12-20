package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
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

      //    2.将一行数据进行拆分, 形成一个一个的单词
      //    扁平化, 将整体拆分成个体的操作
      //    "hello world" => hello, world, hello, world
      val words:RDD[String] = lines.flatMap(_.split(" "))

      //    3.将数据根据单词进行分组, 便于统计
      //    (hello, hello, hello), (world, world)
      val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word=>word)

      // 对分组后的数据进行聚合
      //     (hello, 3), (world, 2)
      val wordToCount = wordGroup.map{
        case ( word, list) => {
          (word, list.size)
        }
      }

      //  5.将转换的结果采集到控制台打印出来
      val array: Array[(String, Int)] = wordToCount.collect()
      array.foreach(println)

      // 2.将一行数据进行拆分，拆成一个一个的效果

      // TODO 关闭连接
      sc.stop()
  }
}

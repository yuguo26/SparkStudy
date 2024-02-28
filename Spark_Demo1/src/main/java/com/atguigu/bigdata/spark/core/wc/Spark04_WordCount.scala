package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount {
  def main(args: Array[String]): Unit = {



    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordCount2(sc)
    sc.stop()
  }

  def wordCount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val groupKey: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordCount: RDD[(String, Int)] = groupKey.mapValues(iter => iter.size)

    wordCount.collect().foreach(println)
  }

  //此过程有shuffle, 效率不高
def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val groupByKeyRDD: RDD[(String, Iterable[Int])] = wordOne.groupByKey()

    groupByKeyRDD.collect().foreach(println)
  }
}

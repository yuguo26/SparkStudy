package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordCount9(sc)
    sc.stop()
  }

  def wordCount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val groupKey: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // mapValues: 原RDD中的Key保持不变, 与新的Value一起组成新的RDD中的元素. 因此, 该函数只适用于元素为KV对的RDD
    val wordCount: RDD[(String, Int)] = groupKey.mapValues(iter => iter.size)

    wordCount.collect().foreach(println)
  }

  //此过程有shuffle, 效率不高
  def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    // groupByKey 必须有key-value 类型
    // groupByKey 此过程有shuffle, 当数据量较大时效率不高
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = wordOne.groupByKey()

    groupByKeyRDD.collect().foreach(println)
  }

  // reduceByKey算子
  def wordCount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)

    wordCount.collect().foreach(println)
  }

  // aggregateByKey
  def wordCount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_, _+_)

    wordCount.collect().foreach(println)
  }

  // combineByKey
  def wordCount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)

    wordCount.collect().foreach(println)
  }

  // combineByKey
  def wordCount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      value => value,
      (x: Int, y) => x + y,
      (x:Int, y:Int) => x + y
    )

    wordCount.collect().foreach(println)
  }

  // countByKey
  def wordCount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val stringToLong: collection.Map[String, Long] = wordOne.countByKey()

    println(stringToLong)
  }

  // countByValue
  def wordCount8(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))

    val stringToLong: collection.Map[String, Long] = words.countByValue()

    println(stringToLong)
  }

  // reduce
  def wordCount9(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))

    // [(word, count), (word, coucnt)]
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )

    val wordCount: mutable.Map[String, Long] = mapWord.reduce(
      (map1, map2) => {
        map2.foreach{
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.updated(word, newCount)
          }
        }
        map1
      }
    )
    println(wordCount)
  }
}

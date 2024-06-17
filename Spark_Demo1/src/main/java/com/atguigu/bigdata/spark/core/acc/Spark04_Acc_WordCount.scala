package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {

  def main(args: Array[String]): Unit = {
    // Spark 封装了三大数据结构,
    // --> RDD: 弹性分布式数据集
    // --> 累加器: 分布式共享只写变量(只写: 累加器之间的值, 互相之间是不能访问到的)
    // --> 广播变量: 分布式共享只读变量
    val sparkConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello", "scala"))

    val reductRDD: RDD[(String, Int)] = rdd.map((_, 1)).reduceByKey(_ + _)

    // 累加器: WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator
    // 向Spark进行注册
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach(
      word => {
        // 数据的累加(使用累加器)
        wcAcc.add(word)
      }
    )

    // 获取累加器累加的结果
    println(wcAcc.value)

    //reductRDD.collect().foreach(println)

    sc.stop()
  }

  //  自定义数据累加器: wordCount
  //  1. 继承 AccumulatorV2, 定义泛型
  //       IN: 累加器输入的数据类型 String
  //       OUT: 累加器返回的数据类型 mutable.Map[String, Long]
  //  2. 重写方法
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{

    private var wcMap = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.updated(word, newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach{
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.updated(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}

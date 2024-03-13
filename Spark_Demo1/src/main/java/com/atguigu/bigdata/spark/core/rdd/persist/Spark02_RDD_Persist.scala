package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDateTime

object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val currentTime: LocalDateTime = LocalDateTime.now()

    val list = List("Hello Scala", "Hello Spark")
    val rdd = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("@@@@@@@@@@@@@@@@@@@@")
        println(currentTime)
        (word, 1)
      }
    )

    // RDD中不存储数据, 持久化存储起来方便后续算子进行计算， 默认情况下会将数据以缓存在jvm的堆内存中
    // 但是并不是这两个方法被调用时立即缓存, 而是触发后面的action算子时, 该RDD将会被缓存在计算节点的内存中,并供后面重用
    //mapRDD.cache()

    // cache 和persist方法相同
    // cache存在内存， 如果需要保存到磁盘需要用到persist
    // 持久化操作必须在行动算子执行时完成的
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("**************************************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    println(currentTime)
    sc.stop()
  }

}

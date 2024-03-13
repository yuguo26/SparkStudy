package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("nba", "*******"),
      ("cba", "*******"),
      ("wnba", "*******"),
      ("nba", "*******"),
    ), 3)
    //new HashPartitioner()

    val parRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    parRDD.saveAsTextFile("output11")
    parRDD.collect().foreach(println)
    sc.stop()
  }

  // 自定义分区器, 决定数据分区的规则
  // 1. 继承Partitioner类
  // 2. 重写方法
  class MyPartitioner extends Partitioner{
    // 分区的数量
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据的分区索引(从0开始)

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }

      if (key == "nba"){
        0
      }else if(key == "wnba"){
        1
      }else if(key == "cba"){
        2
      }else {
        2
      }
    }


  }

}

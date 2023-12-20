package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("RDD")

    // 配置分区数量
    sparkConf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(sparkConf)

    // rdd的并行度 & 分区
    // makeRDD的第二个参数表示分区的数量
    // 第二个参数是可以不传递的, 如果不传递是默认值: defaultParallelism(默认值)
    // spark在默认情况下,从配置对象中获取配置参数: spark.default.parallelism, 如果获取不到,那么使用totalCores属性, 这个属性取值为当前运行环境的最大可用核数
//    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    val rdd = sc.makeRDD(List(1,2,3,4))

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}

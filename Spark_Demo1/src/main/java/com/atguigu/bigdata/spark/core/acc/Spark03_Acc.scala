package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {

  def main(args: Array[String]): Unit = {
    // Spark 封装了三大数据结构,
    // --> RDD: 弹性分布式数据集
    // --> 累加器: 分布式共享只写变量
    // --> 广播变量: 分布式共享只读变量
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器, 申明累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Int] = rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 累加器少加问题: 转化算子中调用累加器, 如果没有行动算子的话, 那么不会执行
    // 累加器多加问题: 转化算子中调用累加器, 如果触发多次行动算子的话, 那么同样的数据也会多次执行
    // 比如本应是10的结果
    // 一般情况下累加器会放置在行动算子中进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)

    sc.stop()
  }

}

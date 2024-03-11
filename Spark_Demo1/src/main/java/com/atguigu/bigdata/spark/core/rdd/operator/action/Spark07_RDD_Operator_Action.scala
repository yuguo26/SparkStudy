package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 行动算子 foreach 分布式遍历RDD中的没一个元素, 调用指定函数
    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))

    // 收集后打印, 这个print是在driver端的内存中打印
    // foreach 其实就是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("***********")
    // 不采集直接打印, 这个print是在excutor端中打印
    // foreach 是Executor端内存数据打印(分布式打印)
    rdd.foreach(println)

    // 算子： Operator(操作)
    //       RDD方法和Scala集合对象的方法不一样
    //       集合对象的方法都是在同一个节点的内存中完成的
    //       RDD 的方法可以将计算逻辑发送到Executor端(分布式节点) 执行
    //       为了区分不同的处理效果, 所以将RDD的方法称之为算子
    //       RDD的方法外部的操作都是在Driver端执行的, 而方法内部的逻辑代码是在Executor执行的
    sc.stop()
  }
}

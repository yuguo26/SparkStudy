package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd: RDD[Any] = sc.makeRDD(List(List(1,2),3,List(4,5)))

    // 扁平化操作
    val mpRDD = rdd.flatMap {
      case x: List[_] => x
      case x:Int => List[Int](x)
    }
    val flatRDD = rdd.flatMap (
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )
    mpRDD.collect().foreach(println)
    println("-------------------------------")
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}

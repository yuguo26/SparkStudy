package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd = sc.makeRDD(List(1,2,3,4))

    // 转换函数
    def mapFunction(num: Int): Int = {
      num * 2
    }

    val mapRDD: RDD[Int] = rdd.map(mapFunction)

    val mapRDD1 : RDD[Int] = rdd.map((num:Int)=>{num * 2})
    val mapRDD2 : RDD[Int] = rdd.map((num:Int)=> num * 2)
    val mapRDD3 : RDD[Int] = rdd.map((num)=> num * 2)
    val mapRDD4 : RDD[Int] = rdd.map(num=> num * 2)
    val mapRDD5 : RDD[Int] = rdd.map(_*2)

    mapRDD.collect().foreach(println)
    //mapRDD1.collect().foreach(println)
    //mapRDD2.collect().foreach(println)
    //mapRDD3.collect().foreach(println)
    //mapRDD4.collect().foreach(println)
    //mapRDD5.collect().foreach(println)

    sc.stop()
  }

}

package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val search = new Search("h")

    //val matchRDD1: RDD[String] = search.getMatchedRDD1(rdd)
    val matchRDD2: RDD[String] = search.getMatchedRDD2(rdd)

    matchRDD2.collect().foreach(println)

    sc.stop()
  }

  // 查询对象
  // Scala中类的构造参数其实是类的属性, 构造参数需要进行闭包检测, 其实就等同于类进行闭包检测
  // class Search(query: String) extends Serializable {
   class Search(query: String) {
    def isMatch(s: String) = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatchedRDD2(rdd: RDD[String]) = {
      val s = query
      // s是方法局部变量, 所以类不需要局部变量
      rdd.filter(_.contains(s))
    }
  }
}

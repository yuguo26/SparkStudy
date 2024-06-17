package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Bc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparkConf)

   val rdd1 =  sc.makeRDD(List(
      ("a", 1),("b", 2),("c", 3)
    ))

    val rdd2 =  sc.makeRDD(List(
      ("a", 4),("b", 5),("c", 6)
    ))

    val map = mutable.Map(("a", 4),("b", 5),("c", 6))

    // join会导致数据量几何增长, 并且会影响shuffle的性能, 不推荐使用
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    //joinRDD.collect().foreach(println)

    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map{
      case (w, c) => {
        // 访问广播变量
        val l: Int = bc.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}

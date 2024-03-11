package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 行动算子 foreach 分布式遍历RDD中的没一个元素, 调用指定函数
    val rdd: RDD[Int] = sc.makeRDD(List(4, 2, 3, 0))
    //val rdd: RDD[Int] = sc.makeRDD(List[Int]())

    val user = new User()

    // foreach 是一个分布式打印操作, 谁先打印谁后打印不确定
    // 算子外部的算法在Driver端执行, 算子内部的代码在Executor端执行
    // RDD算子中传递的函数是会包含闭包操作, 那么就会进行检测功能, 称之为闭包检测
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )
    sc.stop()
  }
  // 不序列化的话, 这样也行
  // 样例类会在编译时, 自动混入序列化特质
  case class User() {
    var age: Int = 30
  }

  //class User extends Serializable {
  //  var age: Int = 30
  //}
}

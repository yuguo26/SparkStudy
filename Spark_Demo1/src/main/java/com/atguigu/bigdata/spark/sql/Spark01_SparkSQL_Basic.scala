package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL 的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    // TODO 执行逻辑操作

    // DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    df.show()

    // DataFrame => SQL
    df.createOrReplaceTempView("user")
    spark.sql("select avg(age) from user").show()

    // DataFrame => DSL,
    // 在使用DataFrame时, 如果涉及到转换操作, 需要引入转换规则
    import spark.implicits._    // spark并不是包名, 而是对象的名称
    df.select("age", "username").show()
    df.select($"age" + 1).show

    // DataSet
    // DataFrame 其实就是特定泛型的DataSet
    val seq = Seq(1,2,3,4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    // RDD <==> DataFrame
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30) ,(2, "list", 40) ,(3, "jerry", 50)))
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df1.rdd


    // DataFrame <==> DataSet
    val ds1: Dataset[User] = df1.as[User]
    val df2: DataFrame = ds1.toDF()

    // RDD <==> DataSet
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD: RDD[User] = ds2.rdd

    // TODO 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}

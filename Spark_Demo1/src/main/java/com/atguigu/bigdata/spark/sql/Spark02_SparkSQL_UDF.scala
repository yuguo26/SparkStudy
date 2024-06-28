package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark02_SparkSQL_UDF {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL 的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //spark.sql("select age, username from user").show

    spark.udf.register("prefixName", (name: String) => {
      "Name:" + name
    })
    spark.sql("select age, prefixName(username) from user").show



    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}

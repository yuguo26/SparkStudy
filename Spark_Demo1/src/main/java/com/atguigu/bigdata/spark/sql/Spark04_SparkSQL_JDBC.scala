package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark04_SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL 的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取Mysql数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "test")
      .load()

    df.show
     //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "test2")
      .mode(SaveMode.Append)
      .save()
    spark.close()
  }


}

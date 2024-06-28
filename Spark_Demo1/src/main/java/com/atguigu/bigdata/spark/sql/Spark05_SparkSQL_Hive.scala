package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark05_SparkSQL_Hive {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL 的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-size.xml文件到classpath下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系(包含MySQL的驱动)
    //spark.sql("show tables").show

    // TODO 关闭环境
    spark.close()
  }


}

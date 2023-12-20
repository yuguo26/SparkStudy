package com.atguigu.bigdata.spark.core.rdd.builder

import com.atguigu.bigdata.spark.core.tools.DeleteTools.dirDel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 将文件作为数据处理的数据源, 默认值
    val rdd:RDD[String] = sc.textFile("datas/1.txt")




    // 删除上一次生成的output文件夹
    val path: File = new File("output")
    dirDel(path)

    // TODO 关闭环境
    sc.stop()
  }
}

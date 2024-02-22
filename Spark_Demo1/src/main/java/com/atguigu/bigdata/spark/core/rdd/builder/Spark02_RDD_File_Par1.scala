package com.atguigu.bigdata.spark.core.rdd.builder

import com.atguigu.bigdata.spark.core.tools.DeleteTools.dirDel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 数据分区的分配
    //  1. 数据以行为单位读取.
    //      spark读取文件, 采用的是hadoop的方式读取, 所以一行一行读取, 和字节数没有关系
    //  2. 数据读取时以偏移量为单位

    //  3. 数据分区的偏移量范围的计算
    val rdd:RDD[String] = sc.textFile("datas/1.txt", 3)

    // 删除上一次生成的output文件夹
    val path: File = new File("output")
    dirDel(path)

    rdd.saveAsTextFile("output")
    // TODO 关闭环境
    sc.stop()
  }
}

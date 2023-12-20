package com.atguigu.bigdata.spark.core.rdd.builder
import com.atguigu.bigdata.spark.core.tools.DeleteTools.dirDel
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 删除上一次生成的output文件夹
    val path: File = new File("output")
    dirDel(path)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }


}




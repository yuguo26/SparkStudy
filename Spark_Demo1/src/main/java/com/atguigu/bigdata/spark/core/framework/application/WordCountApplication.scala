package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.TApplication
import com.atguigu.bigdata.spark.core.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCountApplication extends App with TApplication{

  // 启动应用程序
  start(){
    val controller = new WordCountController()
    controller.dispatch()
  }
}

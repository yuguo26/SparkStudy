package com.atguigu.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  // 线程安全问题: 多线程在执行的过程当中, 对共享内存中, 共享对象的属性修改, 所导致的数据冲突

  // ThreadLocal 可以对线程的内存进行控制, 存储数据, 共享数据
  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc: SparkContext):Unit = {
    scLocal.set(sc)
  }

  def take(): SparkContext = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }

}

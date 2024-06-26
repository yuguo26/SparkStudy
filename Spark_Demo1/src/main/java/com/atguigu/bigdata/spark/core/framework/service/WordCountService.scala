package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.TService
import com.atguigu.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

// 服务层
class WordCountService extends TService{

  private val wordCountDao = new WordCountDao()

  // 数据分析
  def dataAnalysis() = {

    val lines: RDD[String] = wordCountDao.readFile("datas/word.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )

    //  4.对分组后的数据进行聚合
    //     (hello, 3), (world, 2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    //  5.将转换的结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()

    array
  }

}

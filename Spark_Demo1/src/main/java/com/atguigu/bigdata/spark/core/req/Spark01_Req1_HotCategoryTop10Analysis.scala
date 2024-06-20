package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {

    // TODO: TOP10 热门品类
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas//user_visit_action.txt")

    // 2. 统计品类的点击数量: (品类ID, 点击数量)
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)


    // 3. 统计品类的下单数量: (品类ID, 下单数量)
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id=>(id, 1))
      }
    ).reduceByKey(_+_)


    // 4. 统计品类的支付数量: (品类ID, 支付数量)
    val payActionRDD = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id=>(id, 1))
      }
    ).reduceByKey(_+_)


    // 5. 将品类进行排序, 并且取TOP10
    //    先按照点击数量排序, 再按照下单数量排序, 再按照支付数量排序
    //    元祖排序: 先比较第一个, 再比较第二个, 再比较第三个, 以此类推
    //    (品类ID, (点击数量, 下单数量, 支付数量))
    // cogroup = connect + group

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val analysisRDD = cogroupRDD.mapValues{
      case (clickIter, orderIter, payIter) => {

        var clickCnt = 0
        var iter1 = clickIter.iterator
        if(iter1.hasNext){
          clickCnt = iter1.next()
        }

        var orderCnt = 0
        var iter2 = orderIter.iterator
        if(iter2.hasNext){
          orderCnt = iter2.next()
        }

        var payCnt = 0
        var iter3 = payIter.iterator
        if(iter3.hasNext){
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)

    // 6. 将结果采集到控制台打印
    sc.stop()
  }
}

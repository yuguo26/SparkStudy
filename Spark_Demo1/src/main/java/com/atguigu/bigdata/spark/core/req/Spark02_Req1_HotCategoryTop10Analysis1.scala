package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis1 {

  def main(args: Array[String]): Unit = {

    // TODO: TOP10 热门品类
    // 完善一下
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //Q: actionRDD重复使用
    //Q: cogroup性能可能较低


    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas//user_visit_action.txt")
    actionRDD.cache()

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
    // cogroup 有可能存在shuffle

    // 数据流程
    // (品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
    // (品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
    // (品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
    // (品类ID, (点击数量, 下单数量, 支付数量))

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }

    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }

    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    // 将三个数据源合并在一起, 统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)

    // 6. 将结果采集到控制台打印
    sc.stop()
  }
}

package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis {

  def main(args: Array[String]): Unit = {

    // TODO: TOP10 热门品类
    // 完善一下
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // Q: 存在大量的shuffle操作(reduceByKey)
    // reduceByKey 聚合算子, spark会提供优化, 缓存

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas//user_visit_action.txt")

    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )

    actionDataRDD.cache()



    // TODO: 计算分母
    val pageidToCountMap: Map[Long, Long] = actionDataRDD.map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO: 计算分子
    // 根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    // 分组后, 根据访问时间进行排序(升序)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        //[1,2,3,4]
        //[1,2][2,3][3,4]
        //[1-2,2-3,3-4]
        //Sliding: 滑窗
        //[1,2,3,4]
        //[2,3,4]
        //zip: 拉链实现
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageflowIds.map(
          t => {
            (t, 1)
          }
        )
      }
    )

    // ((1,2),1)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)

    // ((1,2),1) => ((1,2),sum)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    // TODO 计算单跳转换率
    // 分子除以分母
    dataRDD.foreach{
      case ((pageid1, pageid2), sum) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)

        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + (sum.toDouble/lon))
      }
    }

    // 5. 将结果采集到控制台打印
    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
           date: String, //用户点击行为的日期
           user_id: Long, //用户的 ID
           session_id: String, //Session 的 ID
           page_id: Long, //某个页面的 ID
           action_time: String, //动作的时间点
           search_keyword: String, //用户搜索的关键词
           click_category_id: Long, //某一个商品品类的 ID
           click_product_id: Long, //某一个商品的 ID
           order_category_ids: String, //一次订单中所有品类的 ID 集合
           order_product_ids: String, //一次订单中所有商品的 ID 集合
           pay_category_ids: String, //一次支付中所有品类的 ID 集合
           pay_product_ids: String, //一次支付中所有商品的 ID 集合
           city_id: Long
  ) //城市 id

}

package com.lina.sparkmall.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.lina.sparkmall.common.model.UserVisitAction
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SessionExtractApp {
  val  extractNum=1000
  def sessionExtract(sessionCount: Long, taskId: String, sessionActionsRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[SessionInfo] = {
    val sessionInfoRdd: RDD[SessionInfo] = sessionActionsRDD.map { case (sessionId, actions) =>
      var maxActionTime: Long = -1L
      var minActionTime: Long = Long.MaxValue
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val keywordBuffer = new ListBuffer[String]()
      val clickBuffer = new ListBuffer[String]()
      val orderBuffer = new ListBuffer[String]()
      val payBuffer = new ListBuffer[String]()
      for (action <- actions) {

        val actionTimeMillSec: Long = format.parse(action.action_time).getTime
        maxActionTime = Math.max(maxActionTime, actionTimeMillSec)
        minActionTime = Math.min(minActionTime, actionTimeMillSec)
        //判断每个action的操作类型，多个action进行合并
        if (action.search_keyword != null) {
          keywordBuffer += action.search_keyword
        } else if (action.click_product_id != -1L) {
          clickBuffer += action.click_product_id.toString
        } else if (action.order_product_ids != null) {
          orderBuffer += action.order_product_ids
        } else if (action.pay_product_ids != null) {
          payBuffer += action.pay_product_ids
        }

      }
      val visitLength: Long = maxActionTime - minActionTime
      val stepLength: Int = actions.size
      //开始时间
      val startTime: String = format.format(new Date(minActionTime))
      SessionInfo(taskId, sessionId, startTime, stepLength.toLong, visitLength, keywordBuffer.mkString(","), clickBuffer.mkString(","), orderBuffer.mkString(","), payBuffer.mkString(","))
    }

    val dayHourSessionsRDD: RDD[(String, SessionInfo)] = sessionInfoRdd.map { sessionInfo =>
      val dayHourKey: String = sessionInfo.startTime.split(":")(0)
      (dayHourKey, sessionInfo)
    }

    val dayHourSessionGroupRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionsRDD.groupByKey()
    val sesssionExtractRDD: RDD[SessionInfo] = dayHourSessionGroupRDD.flatMap { case (dayHourKey, itrSessions) =>
      //1确定抽取的个数  公式：    当前小时的session数 / 总session数  * 一共要抽取的数
      val dayhourNum: Long = Math.round(itrSessions.size / sessionCount.toDouble * extractNum)
      //2 按照要求的个数进行抽取
      //      =》抽取
      //    RDD[day_hour, Iterable[sessionInfo]]
      val sessionSet: mutable.HashSet[SessionInfo] = randomExtract(itrSessions.toArray, dayhourNum)
      sessionSet
    }
    sesssionExtractRDD
  }
  def randomExtract[T]( arr: Array[T],num:Long)={
    //随机产生一个下标值（0 -> arr.length）
    val resultSet = new mutable.HashSet[T]()
    //抽到满足为止
    while(resultSet.size<num){
      val index: Int = new Random().nextInt(arr.length)
      val value = arr(index)
      resultSet+=value
    }
    resultSet
  }
}
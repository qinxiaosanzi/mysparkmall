package com.lina.sparkmall.offline

import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.lina.sparkmall.common.ConfigurationUtil
import com.lina.sparkmall.common.model.UserVisitAction
import com.lina.sparkmall.common.util.JdbcUtil
import com.lina.sparkmall.offline.utils.SessionAccumulator
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.rdd.RDD

import scala.collection.mutable
object Offline {

  def main(args: Array[String]): Unit = {
    val sparkConf :SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
    val sparkSession :SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val conditionConfig = ConfigurationUtil("condition.properties").config
    val conditionJsonString = conditionConfig.getString("condition.params.json")
    val userActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession,conditionJsonString)
    //以session_id进行分组，
    val userSessionRDD = userActionRDD.map(serAction => (serAction.session_id,serAction)).groupByKey()
    val userSessionCount = userSessionRDD.count()
    println(userSessionCount)
    //求出每个session的时长，根据最大action_time - 最小action_time
    val accumulator = new SessionAccumulator
    sparkSession.sparkContext.register(accumulator)
    userSessionRDD.foreach{case (sessionId,actions)  =>
      var maxActionTime:Long = -1L
      var minActionTime:Long = Long.MaxValue
      for(action <- actions){
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val actionTimeMillSec:Long = format.parse(action.action_time).getTime
        maxActionTime = Math.max(maxActionTime,actionTimeMillSec)
        minActionTime = Math.min(minActionTime,actionTimeMillSec)
      }
      val visitTime = maxActionTime - minActionTime
      if(visitTime <= 10000){
        //累加器
        accumulator.add("session_visitLength_le_10_count")

      }else {
        //累加器
        accumulator.add("session_visitLength_gt_10_count")
      }

      if(actions.size <= 5){
        //累加器
        accumulator.add("session_stepLength_le_5_count")
      }else{
        //累加器
        accumulator.add("session_stepLength_gt_5_count")
      }
    }

    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value

    //6 把累计值计算为比例
    val session_visitLength_gt_10_ratio = Math.round(1000.0 * sessionCountMap("session_visitLength_gt_10_count") / userSessionCount) / 10.0
    val session_visitLength_le_10_ratio = Math.round(1000.0 * sessionCountMap("session_visitLength_le_10_count") / userSessionCount) / 10.0
    val session_stepLength_gt_5_ratio = Math.round(1000.0 * sessionCountMap("session_stepLength_gt_5_count") / userSessionCount) / 10.0
    val session_stepLength_le_5_ratio = Math.round(1000.0 * sessionCountMap("session_stepLength_le_5_count") / userSessionCount) / 10.0
    val taskId: String = UUID.randomUUID().toString
    val resultArray = Array(taskId, conditionJsonString, userSessionCount, session_visitLength_gt_10_ratio, session_visitLength_le_10_ratio, session_stepLength_gt_5_ratio, session_stepLength_le_5_ratio)
    //7 保存到mysql中
    JdbcUtil.executeUpdate("insert into session_stat_info values (?,?,?,?,?,?,?) ", resultArray)

    //需求 按比例抽取session
    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExtract(userSessionCount ,taskId,userSessionRDD)

    import sparkSession.implicits._
    val  config: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    sessionExtractRDD.toDF.write.format("jdbc")
      .option("url",config.getString("jdbc.url"))
      .option("user",config.getString("jdbc.user"))
      .option("password",config.getString("jdbc.password"))
      .option("dbtable","random_session_info")
      .mode(SaveMode.Append).save()

  }
/*  def  asRequired(session_id: String,actions: Iterable[UserVisitAction]): Unit ={
    var maxActionTime:Long = -1L
    var minActionTime:Long = Long.MaxValue
    for(action <- actions){
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val actionTimeMillSec:Long = format.parse(action.action_time).getTime
      maxActionTime = Math.max(maxActionTime,actionTimeMillSec)
      minActionTime = Math.min(minActionTime,actionTimeMillSec)
      val visitTime = maxActionTime - minActionTime
      if(visitTime <= 10000){
      //累加器
      }else {
        //累加器
      }

      if(actions.size <= 5){
        //累加器
      }else{
        //累加器
      }
    }
  }*/

  def readUserVisitActionRDD(sparkSession: SparkSession,conditionJsonString:String): RDD[UserVisitAction] ={
    val config   = ConfigurationUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")
    val jsonObject = JSON.parseObject(conditionJsonString)
    sparkSession.sql("use " +databaseName)
    val sql = new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id = u.user_id where 1=1 ")

    sql.append("and date >= '" + jsonObject.getString("startDate") + "'" )
    sql.append("and date <= '" + jsonObject.getString("endDate") + "'")
    sql.append("and u.age >= '" + jsonObject.getString("startAge") + "'" )
    sql.append("and u.age <= '" + jsonObject.getString("endAge") + "'" )
    import sparkSession.implicits._
    val returnUserVisitRDD:RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
    returnUserVisitRDD
  }



}

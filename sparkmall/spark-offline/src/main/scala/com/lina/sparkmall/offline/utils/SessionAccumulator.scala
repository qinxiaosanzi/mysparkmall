package com.lina.sparkmall.offline.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable



class SessionAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  var sessionMap = new mutable.HashMap[String,Long]()
  override def isZero: Boolean = {
    sessionMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new SessionAccumulator
    accumulator.sessionMap ++=sessionMap
    accumulator
  }

  override def reset(): Unit = {
    sessionMap = new mutable.HashMap[String,Long]()
  }

  override def add(key: String): Unit = {
    sessionMap(key) = sessionMap.getOrElse(key,0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherSessionMap = other.value
    sessionMap = sessionMap.foldLeft(otherSessionMap){ case (otherSessionMap,(key,count)) =>
      otherSessionMap(key) = otherSessionMap.getOrElse(key,0L) + count
      otherSessionMap
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    sessionMap
  }
}

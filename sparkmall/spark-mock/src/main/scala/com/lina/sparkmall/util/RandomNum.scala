package com.lina.sparkmall.util

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object RandomNum {

  def main(args: Array[String]): Unit = {
    val sss = multi(1,5,3,",",false)
    println(sss)
  }
  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
/*  // RandomNum.multi(1, cargoryNum, RandomNum(1, 5), ",", false)
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    var myValue :String = ""
    val list = new ArrayBuffer[String]
    var i = 0
    while(i < amount){
      i += 1
      val number = RandomNum(fromNum,toNum).toString
      if(canRepeat){
        myValue += number + delimiter
        list.append(number)
      }else{
        if(isOk(number,list)){
          myValue += number + delimiter
          list.append(number)
        }else{
          i -= 1
        }
      }
    }
    myValue
  }

  def isOk(number: String,list: ArrayBuffer[String]): Boolean ={
    for (i <- 1 to list.length-1){
      if(number.equals(list(i)))return false
    }
    return true
  }*/
def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
  if(canRepeat){
    val valueList = new ListBuffer[Int]
    for(i <- 1 to amount){
      val number = RandomNum(fromNum,toNum)
      valueList += number
    }
    println(valueList)
    valueList.mkString(delimiter)

  }else{
    val valueSet = new mutable.HashSet[Int]
    while (valueSet.size < amount){
      val number = RandomNum(fromNum,toNum)
      valueSet += number
    }
    valueSet.mkString(delimiter)
  }
}

}
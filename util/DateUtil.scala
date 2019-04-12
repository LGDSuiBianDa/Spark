package com.yd.spark.util

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
  * Created by SuiBianDa LGD on 2019/2/17 11:23
  * Modified by: 
  * Version: 0.0.1
  * Usage: process time
  *
  */
object DateUtil extends Serializable {


  /**
    * 获取当前系统时间，时间格式为：yyyy-MM-dd HH:mm:ss.SSS
    * @return yyyy-MM-dd HH:mm:ss.SSS
    */
  def getNowDate:String={
    val now = new Date()
    now.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val result = dateFormat.format( now )
    result
  }

  /**
    * 获取昨天日期
    * @return
    */
  def getYesterday():String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)
    yesterday
  }

  /**
    * 计算时间差
    * @param start_time
    * @param end_Time
    * @return
    */
  def getDiffTime(start_time:String,end_Time:String)={
    val df:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val df:SimpleDateFormat=new SimpleDateFormat("HH:mm:ss")
    val begin:Date=df.parse(start_time)
    val end:Date = df.parse(end_Time)
    val between:Long=(end.getTime-begin.getTime)/1000//转化成秒
    val hour:Float=between.toFloat/3600
    val decf:DecimalFormat=new DecimalFormat("#0.00")
    decf.format(hour)//格式化
  }

  /**
    * 指定日期和间隔天数，返回指定日期前N天的日期 date - N days
    * @param dt
    * @param interval
    * @return
    */
  def getDaysBefore(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)

    cal.add(Calendar.DATE, - interval)
    val yesterday = dateFormat.format(cal.getTime)
    yesterday
  }

  /**
    * 指定日期和间隔小时，返回指定日期后N小时的日期 date - N days
    * @param dt
    * @param interval
    * @return
    */
  def getHoursAfter(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)

    cal.add(Calendar.HOUR, + interval)
    val yesterday = dateFormat.format(cal.getTime)
    yesterday
  }

  /**
    * 指定日期和间隔小时，返回指定日期前N小时的日期 date - N days
    * @param dt
    * @param interval
    * @return
    */
  def getHoursBefore(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)

    cal.add(Calendar.HOUR, - interval)
    val yesterday = dateFormat.format(cal.getTime)
    yesterday
  }

  /**
    * 时间转换为时间戳
    * @param tm
    * @return
    */
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime
    tim
  }

  /**
    * 时间戳转化为时间
    * @param tm
    * @return
    */
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}

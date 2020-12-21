package com.yd.spark.util

import org.apache.spark.SparkConf

import scala.runtime.ObjectRef

object SparkUtil {

  /**
    * initialize spark conf
    * @return
    */
  def initSparkConf():SparkConf ={

    val pattern = PropertiesLoader.getInstance().getProperty("spark.local.enable")
    val isLocal = if(pattern != null) false else true
    val sparkConf = new SparkConf()
//    sparkConf.set("spark.sql.shuffle.partitions","200")
//    sparkConf.set("spark.default.parallelism","200")
//    sparkConf.set("spark.locality.wait.node","500ms")
//    sparkConf.set("spark.locality.wait.rack","500ms")
//    sparkConf.set("spark.locality.wait.process","2000ms")
    sparkConf.set("spark.locality.wait","100ms")

    /**
      * set streaming config
      */
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")//优雅的关闭
    sparkConf.set("spark.streaming.backpressure.enabled","true")//激活削峰功能
    sparkConf.set("spark.streaming.backpressure.initialRate","30000")//第一次读取的最大数据值
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","20000")//每个进程每秒最多从kafka读取的数据条数

    /**
      * set es config
      */
    sparkConf.set("es.mapping.date.rich","false")
    sparkConf.set("es.index.auto.create","true")
    sparkConf.set("es.nodes.wan.only","true")
    sparkConf.set("es.nodes", PropertiesLoader.getInstance().getProperty("es.host"))
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.write.operation","upsert")

    /**
      * set hive config
      */
    if(isLocal) sparkConf.setMaster("local[*]").setAppName("local-test")
    sparkConf
  }
}

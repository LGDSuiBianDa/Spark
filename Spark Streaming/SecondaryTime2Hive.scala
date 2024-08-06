package com.rd.spark.streaming

import com.rd.kafka.check.util.AuthUtil
import com.rd.spark.util.{DateUtil, PropertiesLoader, SparkUtil}
import kafka.api.OffsetRequest
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SecondaryTime2Hive {

  val logger = org.apache.log4j.LogManager.getLogger("SecondaryTime2Hive")

  def main(args: Array[String]): Unit = {


    /**
      * get broker、topicName from Sink kafka
      */
    val monitorUrl = PropertiesLoader.getInstance().getProperty("second.kafkaspout.monitorUrl")
    val clusterName = PropertiesLoader.getInstance().getProperty("second.kafkaspout.clusterName")
    val topicToken = PropertiesLoader.getInstance().getProperty("second.kafkaspout.token")
    val broker = AuthUtil.getBrokers(clusterName,topicToken,monitorUrl)
    val topic = topicToken.split(":")(0)

    val checkpointDirectory = "drcs_spark/secondaryTimeCheckPointDir"
    //create StreamingContext
    val ssc=createStreamingContext(broker,topic,checkpointDirectory)
    //start
    ssc.start()
    //block
    ssc.awaitTermination()
  }


  def createStreamingContext(broker:String,topic:String,checkpointDirectory:String):StreamingContext={

    //    // If you do not see this printed, that means the StreamingContext has been loaded
    //    // from the new checkpoint
    val firstReadLastest=true//第一次启动是否从最新的开始消费

    val sparkConf = SparkUtil.initSparkConf()

    /**
      * sparkConf配置
      */
    sparkConf.set("spark.default.parallelism","300")


    /**
      * 创建StreamingContext,每隔多少秒一个批次
      */
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc=new StreamingContext(sc,Seconds(30))//创建StreamingContext,每隔多少秒一个批次
    //    ssc.checkpoint("drcs_spark/deduceOnlineServiceDir")

    /**
      * kafkaParams配置
      */
    var kafkaParams = Map[String,String]("bootstrap.servers"-> broker)//创建一个kafkaParams
    if (firstReadLastest)   kafkaParams += ("auto.offset.reset"-> OffsetRequest.SmallestTimeString)//从最新的开始消费


    /**
      * 创建Dstream
      */
    val original_Dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSeq.toSet)
    val result_Dstream = original_Dstream.map(_._2).map{t=>

      var type_label = ""
      var key_no = ""
      var promise_tm = ""
      var arrive_tm = ""
      var rowkey = ""
      var request_tm = ""
      var departure_tm = ""
      var predict = ""
      var plan_delivery_time = ""
      var buffer = ""
      val inc_day = DateUtil.getNowDate.split(" ")(0).replace("-","")
      try{
        val fields = t.split(",",-1)
        type_label = fields(0)
        key_no = fields(1)
        promise_tm = fields(2)
        arrive_tm = fields(3)
        rowkey = fields(4)
        request_tm = fields(5)
        departure_tm = fields(6)
        predict = fields(7)
        plan_delivery_time = fields(8)
        buffer = fields(9)
      }catch {case e:Exception=>
        logger.error(s"kafka data :{ $t } error msg = ${e.getMessage}")
      }
      SecondaryData(type_label,key_no,promise_tm,arrive_tm,rowkey,
        request_tm,departure_tm,predict,plan_delivery_time,buffer,inc_day)
    }.filter(_.key_no.length>1)


//    result_Dstream.print(10)


    result_Dstream.foreachRDD{rdd =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).enableHiveSupport().getOrCreate()

      // 如果rdd有数据
      if (!rdd.isEmpty()) {
        // 在每个Partition里执行
        // Turn on flag for Hive Dynamic Partitioning
        spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
        spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        val hive_data = spark.createDataFrame(rdd).coalesce(1)
        hive_data.show(1)
        hive_data.write.mode(SaveMode.Append).partitionBy("inc_day").format("hive").saveAsTable("database1.zzz_inc_amds_core_secondary_promise_data")
//        rdd.foreachPartition{partitions =>
//
//          // 使用广播变量发送到Kafka
//          partitions.foreach{record =>
//
//            /**
//              * save data to hive table
//              */
//            // Turn on flag for Hive Dynamic Partitioning
//            spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
//            spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//            val hive_data = spark.createDataFrame(List(record)).coalesce(1)
//            hive_data.show(5)
////            hive_data.write.mode(SaveMode.Append)./*partitionBy("route_code").*/format("hive").saveAsTable("tmp_database1.yzq_outline_info")
//            logger.error("读取的数据："+record)
//            }
//          }

        }
    }


    ssc//return StreamContext

  }


  case class SecondaryData(type_label:String,key_no:String,promise_tm:String,arrive_tm:String,rowkey:String,
                           request_tm:String,departure_tm:String,predict:String,plan_delivery_time:String,buffer:String,inc_day:String)


}

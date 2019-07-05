package com.yd.spark

import java.util.Properties

import com.yd.spark.util.KafkaSink
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

object StreamingToKafkaTest {
  private val LOG = LoggerFactory.getLogger("StreamingToKafkaTest")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingToKafkaTest")
    // batchDuration 设置为 1 秒，然后创建一个 streaming 入口
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)


    val bootstrapServers = "host1:port,host2:port"
    val topicName = "XXXXX"
//    val maxPoll = 1000


    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", bootstrapServers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      if (LOG.isInfoEnabled)
        LOG.info("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    words.foreachRDD{rdd =>

      // 如果rdd有数据
      if (!rdd.isEmpty()) {
        // 在每个Partition里执行
        rdd.foreachPartition{partitions =>

            // 使用广播变量发送到Kafka
          partitions.foreach{record =>
            var seMap = mutable.Map[String,Any]()
            seMap += (record._1 -> record._2)
              kafkaProducer.value.send(topicName, JSONObject(seMap.toMap).toString)
            }

          }
      }
    }

    ssc.start()

    ssc.awaitTermination()

  }

}

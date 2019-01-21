package com.sf.spark.streaming

import org.apache.spark.sql.SparkSession

/**
  * Created by suibianda LGD  on 2019/1/21 15:59
  * Modified by: 
  * Version: 0.0.1
  * Usage: 
  *
  */
object StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions",50)
    
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}

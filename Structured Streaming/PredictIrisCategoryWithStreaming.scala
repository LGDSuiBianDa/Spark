package com.yd.spark.stream


import com.yd.spark.util.PropertiesLoader
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.ml.tuning._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager, Logger}

/**
  * @ClassName: PredictIrisCategoryWithStreaming.scala
  * @Description:
  *              Consumes messages from a topic of Kafka,
  *              enriches the message with  the k-means model cluster id and publishs the result in json format
  *              to another kafka topic
  * @Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
  *         <model>  is the path to the saved model
  *         <topics> is a  topic to consume from
  *         <topicp> is a  topic to publish to
  * Example:
  *     $  spark-submit --class com.sparkkafka.flight.SparkKafkaConsumerProducer --master local[2] \
  * dataExplore-1.0.jar /user/user01/data/savemodel  topicssubscribe topicspublish
  * @Auther:suibianda LGD
  * @Date: 2019/4/25 下午6:00
  * @version : V1.0
  */
object PredictIrisCategoryWithStreaming {

  val log = LogManager.getLogger("PredictIrisCategoryWithStreaming")

  // schema for iris data
  case class Iris(sepalLength:Double, sepalWidth:Double, petalLength:Double, petalWidth:Double, category:String)
    extends Serializable

  val schema = StructType(Array(
    StructField("sepalLength", DoubleType, true),
    StructField("sepalWidth", DoubleType, true),
    StructField("petalLength", DoubleType, true),
    StructField("petalWidth", DoubleType, true),
    StructField("category", StringType, true)
  ))

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.spache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spache.kafka").setLevel(Level.WARN)


    var modelpath = "/Users/xxx/decisionTreeModel"
    var topics = PropertiesLoader.getInstance().getProperty("subscribe.topic.name")
    var topicp = PropertiesLoader.getInstance().getProperty("publish.topic.name")
    val brokers = PropertiesLoader.getInstance().getProperty("kafka.brokers")
//    val groupId = "sparkApplication"
//    val pollTimeout = "10000"

    if (args.length == 3) {
      modelpath = args(0)
      topics = args(1)
      topicp = args(2)
    } else {
      System.out.println("please specify the parameter, including model path, subscribe topic and publish topic. " +
        "For example /user/user01/data/cfModel topics topicsp ")
    }

    System.out.println("Use model " + modelpath + " Subscribe to : " +brokers+" topic: "+ topics + " Publish to: " + topicp)

    /**
      * define sparksession
      */
    val spark = SparkSession.builder().master("local[3]").appName("PredictIrisCategoryWithStreaming").getOrCreate()/*sit*/
//    val spark = SparkSession.builder().getOrCreate()/*Pro*/
    spark.conf.set("spark.sql.shuffle.partitions",200)
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    /**
      * define UDF, deserialize Array[Byte]
      */
    object MyDeserializerWrapper{
      val deser = new ByteArrayDeserializer
    }
    spark.udf.register("deserialize",(topic:String, bytes:Array[Byte])=>MyDeserializerWrapper.deser.deserialize(topic,bytes))

    /**
      * create dataframe representing the stream of input kafka topic logs
      */
    val messagesStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()
    /**
      * mac socket 方式的 host、port 不知道如何设置
      */
//    val messagesStream = spark.readStream
//      .format("socket")
//      .option("host","SF001376849MAC.local")
//      .option("port",9999)
//      .load()

    /**
      * load model
      */
    val model = CrossValidatorModel.load(modelpath)

    /**
      * get value from kafka, transform to dataset[className/String]
      */
    val valuesStream = messagesStream.selectExpr("cast (value as string) as json").select(from_json($"json", schema) as "data")
      .select($"data.sepalLength", $"data.sepalWidth", $"data.petalLength", $"data.petalWidth", $"data.category").as[Iris]
//    val valuesStream = messagesStream.selectExpr("""deserialize(topics,value) as json""").as[Flight]

    // get cluster categories from  model
    val predictions = model.transform(valuesStream)
    println("show dstream dataset received with predictions ")
//    predictions.show()
    //  categories.show
    predictions.createOrReplaceTempView("iriss")

    // convert results to JSON string to send to topic

    val lp = predictions.select($"sepalLength", $"sepalWidth", $"petalLength", $"petalWidth", $"category",
      $"prediction".alias("pred_dtree"))
    println("show predictions fordataframe received ")

//    lp.show
    lp.createOrReplaceTempView("iris")

    println("what is the count of category  by sepalLength")

//    spark.sql("select pred_dtree, count(pred_dtree) from iris group by pred_dtree order by pred_dtree")


    // Start the computation and prints the result to the console
    val query = lp.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }



}

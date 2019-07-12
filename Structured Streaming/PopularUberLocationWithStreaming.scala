package com.yd.spark.stream

import java.text.SimpleDateFormat

import com.yd.spark.util.PropertiesLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType

/**
  * @ClassName: PopularUberLocationWithStreaming
  * @Description:
  * @Usage:
  * @Auther: suibianda LGD
  * @Date: 2019/7/11 下午3:51
  * @version : V1.0
  */
object PopularUberLocationWithStreaming {

  case class Center(cid:Integer,clat:Double,clon:Double)

  case class Uber(dt:String,lat:Double,lon:Double,base:String,rdt:Long)

  //Uber class with cluster id,lat lon
  case class UberC(dt:java.sql.Timestamp,lat:Double,lon:Double,
                   base:String,rdt:Long,cid:Integer,clat:Double,clon:Double) extends Serializable

  //Uber with unique Id and cluster id and cluster lat lon
  case class UberwId(_id:String,dt:java.sql.Timestamp,lat:Double,lon:Double,
                    base:String,cid:Integer,clat:Double,clon:Double)

  def parseUber(str:String):Uber={
    val p = str.split(",",-1)
    val date = p(0).split(" ")(0).split("\\/",-1)
    val time = p(0).split(" ")(1).split(":",-1)
    val hour = if(time(0).length == 1) "0"+time(0) else time(0)
    val min = time(1)
    val sec = time(2)
    val tim = date(2)+"-"+date(0)+"-"+date(1)+" "+hour + ":" + min +":"+sec
    val dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rdt = dfm.parse(tim).getTime.toString.reverse.toLong
    Uber(tim,p(1).toDouble,p(2).toDouble,p(3),rdt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.spache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spache.kafka").setLevel(Level.WARN)

    var saveModeldir = "/data/kmeansModel"
    var topics = PropertiesLoader.getInstance().getProperty("topic.name")
    var topicp = "/user/user01/stream:flightp"
    val brokers = PropertiesLoader.getInstance().getProperty("kafka.brokers")
    //    val groupId = "sparkApplication"
    //    val pollTimeout = "10000"

    if (args.length == 3) {
      saveModeldir = args(0)
      topics = args(1)
      topicp = args(2)
    } else {
      System.out.println("Using hard coded parameters unless you specify the model path, subscribe topic and publish topic. " +
        "For example /user/user01/data/cfModel /user/user01/stream:flights /user/user01/stream:flightp ")
    }

    System.out.println("Use model " + saveModeldir + " Subscribe to : " +brokers+" topic: "+ topics + " Publish to: " + topicp)

    val spark = SparkSession.builder()
      .master("local[3]").appName("PopularUberLocationWithStreaming")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val model = KMeansModel.load(saveModeldir)

    var ac = new Array[Center](20)
    var index:Int = 0
    model.clusterCenters.foreach(x=>{
      ac(index) = Center(index, x(0), x(1))
      index += 1
    })

    val ccdf = spark.createDataset(ac)
    ccdf.show(3)

    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("maxOffsetsPerTrigger",1000)
      .load()

//    df1.printSchema()

    spark.udf.register("deserialize",
      (message:String) => parseUber(message))

    val df2 = df1.selectExpr("""deserialize(CAST(value as STRING)) AS message""")
      .select($"message".as[Uber])
    df2.printSchema()
    df2.show(5)

    // input column names
    val featureCols = Array("lat", "lon")
    // create transformer
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    // transform method adds features column
    val df3 = assembler.transform(df2)
    //use model to get the clusters from the features
    val clustersl = model.transform(df3)

    val temp = clustersl.select($"dt".cast(TimestampType),
      $"lat",$"lon",$"base",$"rdt",$"prediction".alias("cid"))

    val clusters = temp.join(ccdf,Seq("cid")).as[UberC]

    //enrich with unique id
    def createUberwId(uber:UberC):UberwId = {
      val id = uber.cid + "_" + uber.rdt
      UberwId(id,uber.dt,uber.lat,uber.lon,uber.base,uber.cid,uber.clat,uber.clon)
    }
    val cdf = clusters.map(uber=>createUberwId(uber))

    //writing to a sink
    val query = cdf.writeStream
      .queryName("uber")
//      .format("memory")
      .format("console")
      .outputMode("append")
      .start()

     query.awaitTermination()




  }

}

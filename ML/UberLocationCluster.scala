package com.yd.spark.ml


import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
/**
  * @ClassName: UberLocationCluster
  * @Description:
  * @Usage: use KMeans to cluster the uber location
  * @Auther: suibianda LGD 
  * @Date: 2019/7/9 下午3:52
  * @version : V1.0
  */
object UberLocationCluster {


  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable
  case class Uber1(dt: java.sql.Timestamp, lat: Double, lon: Double, base: String) extends Serializable
  case class Uber2(dt: String, lat: Double, lon: Double, base: String,rdt:Long) extends Serializable

  val schema = StructType(Array(
    StructField("dt", StringType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))

  def parseUber(str:Uber):Uber2={
    val p = str.dt
    val date = p.split(" ")(0).split("\\/",-1)
    val time = p.split(" ")(1).split(":",-1)
    val hour = if(time(0).length == 1) "0"+time(0) else time(0)
    val min = time(1)
    val sec = time(2)
    val tim = date(2)+"-"+date(0)+"-"+date(1)+" "+hour + ":" + min +":"+sec
    val dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rdt = dfm.parse(tim).getTime.toString.reverse.toLong
    Uber2(tim,str.lat,str.lon,str.base,rdt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.spache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spache.kafka").setLevel(Level.WARN)

    val spark = SparkSession.builder().master("local[3]").appName("uber").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val file = "data/uber-raw-data-*.csv"
    import spark.implicits._

    spark.udf.register("deserialize",
      (message:Uber) => parseUber(message))

    val df = spark.read.option("inferSchema","false")
      .option("header","true").schema(schema).csv(file).as[Uber]
      .map(x=>parseUber(x))
      .select($"dt".cast(TimestampType),$"lat",$"lon",$"base",$"rdt")


    df.printSchema()
    df.show(5)

    // input column names
    val featureCols = Array("lat", "lon")
    // create transformer
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    // transform method adds features column
    val df2 = assembler.transform(df)
    // cache transformed DataFrame
    df2.cache
    df2.show(5)

    // create the estimator
    val kmeans: KMeans = new KMeans()
      .setK(20)
      .setFeaturesCol("features")
      .setPredictionCol("cid")
      .setSeed(1L)
    // use the estimator to fit (train) a KMeans model
    val model: KMeansModel = kmeans.fit(df2)
    // print out the cluster center latitude and longitude
    println("Final Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)


    // get the KMeansModelSummary from the KMeansModel
    val summary:KMeansSummary  = model.summary
    // get the cluster centers in a dataframe column from the summary
    val clusters  = summary.predictions
    // register the DataFrame as a temporary table
    clusters.createOrReplaceTempView("uber")
    clusters.show(5)

    clusters.groupBy("cid").count().orderBy(desc( "count")).show(5)

    clusters.select(hour($"dt").alias("hour"), $"cid") .groupBy("hour", "cid").agg(count("cid") .alias("count")).orderBy(desc("count"),$"hour").show(5)

    val modeldir = "/data/kmeansModel"
    model.write.overwrite().save(modeldir)
//    val sameModel = KMeansModel.load(modeldir)

  }

}

package com.yd.spark.ml

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object PredictTimeArrivalWithRF {

  /**
    * define schema of cleaned data
    */
  val schema = StructType(Array(
    StructField("id", StringType, true),
    StructField("limit_type", StringType, true),
    StructField("cargo_type", StringType, true),
    StructField("product_code", StringType, true),
    StructField("pickupCode", IntegerType, true),
    StructField("origin", StringType, true),
    StructField("originT", StringType, true),
    StructField("dest", StringType, true),
    StructField("destT", StringType, true),
    StructField("transType", StringType, true),
    StructField("apattachinfo", StringType, true),
    StructField("dofW", IntegerType, true),
    StructField("dephour", IntegerType, true),
    StructField("deptime", DoubleType, true),
    StructField("diff", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))

  case class SessionBetween(
                             id:String,
                             limit_type:String,
                             cargo_type:String,
                             product_code:String,
                             pickupCode:Int,
                             origin:String,
                             originT:String,
                             dest:String,
                             destT:String,
                             transType:String,
                             apattachinfo:String,
                             dofW:Int,
                             dephour:Int,
                             deptime:Double,
                             diff:Double,
                             dist:Double) extends Serializable

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("PredictTimeArrivalWithRF")
      .master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /**
      * load  data from file,transform to a DataSet[SessionBetween]
      */
    val file = "/users/xxx.csv"
    import spark.implicits._
    val df = spark.read
      .option("inferSchema","false")
      .option("header","true")
      .schema(schema)
      .csv(file)
      .as[SessionBetween]
    df.printSchema()
    df.show(5)
    println(s"total num:  ${df.count()}")

    val strain = df
//        .filter($"diff" isNotNull)
//        .sample(false,0.5,5043)
//    strain.show(20)

    /**
      * load model
      */
    val modeldir = "/users/predictTimeArrivalRfModel"
    val model = CrossValidatorModel.load(modeldir)

    //get prediction from model
    val predictions = model.transform(strain)
    println(s"show dataset received with predictions ")

    predictions.printSchema()
    predictions.cache()
    predictions.show(5)
    predictions.createOrReplaceTempView("eta")
    val counttotal = predictions.count()
    val count5 =  spark.sql("select transType,pickupCode,limit_type,prediction,diff,(prediction-diff) error from eta where abs(prediction-diff) <= 5").count()
    val count10 =  spark.sql("select transType,pickupCode,limit_type,prediction,diff,(prediction-diff) error from eta where abs(prediction-diff) <= 10").count()
    val count15 =  spark.sql("select transType,pickupCode,limit_type,prediction,diff,(prediction-diff) error from eta where abs(prediction-diff) <= 15").count()
    val count20 =  spark.sql("select transType,pickupCode,limit_type,prediction,diff,(prediction-diff) error from eta where abs(prediction-diff) <= 20").count()

    spark.sql("select * from (select *,abs(prediction-diff) error from eta)t order by error desc").show(20)

    val count5percent = count5.toDouble/counttotal.toDouble
    val count10percent = count10.toDouble/counttotal.toDouble
    val count15percent = count15.toDouble/counttotal.toDouble
    val count20percent = count20.toDouble/counttotal.toDouble
    println(s"count5percent: $count5percent " +
      s",count10percent: $count10percent " +
      s", count15percent: $count15percent " +
      s",count20percent: $count20percent " +
      s",counttotal(testData): $counttotal")



    val rmse = model.getEvaluator.evaluate(predictions)
    println(s"rmse on test data: $rmse")

  }

}

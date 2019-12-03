package com.yd.spark.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object TimeArrivalWithRF {

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

    //load cleaned data from file, converting it to a DataSet
    val file = "/Users/xxx.csv"
    import spark.implicits._
    val df = spark.read
      .option("inferSchema","false")
      .option("header","true")
      .schema(schema)
      .csv(file)
      .as[SessionBetween]
    df.printSchema()
    df.show(5)

    //feature Engineering
    val strain = df

    strain.cache()
    strain.printSchema()
    strain.show(5)

    //split the data into training and test sets (30% held out for testing)
    val Array(trainingData,testData) = strain.randomSplit(Array(0.99,0.01),5043)

    val categoricalColumns = Array("limit_type","cargo_type","product_code","pickupCode","originT","destT","transType")
    val stringIndexer = categoricalColumns.map{colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .setHandleInvalid("skip")//如果转换模型（关系）是基于上面数据得到的 (a,b,c)->(0.0,2.0,1.0),如果用此模型转换category多于（a,b,c)的数据，比如多了d，e. 会抛出异常,要忽略这些label所在行的数据需要设置该参数为skip，这样就可以正常运行
        .fit(strain)
    }



    val featureCols = Array("limit_typeIndexed",
      "cargo_typeIndexed",
      "product_codeIndexed",
      "pickupCodeIndexed",
      "originTIndexed",
      "destTIndexed",
      "transTypeIndexed","dofW","dephour","deptime","dist")


    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    //Train a RandomForestRegressor model
    val rf = new RandomForestRegressor()
      .setLabelCol("diff")
      .setFeaturesCol("features")

    //Chain indexers and rf in a Pipeline
    val steps = stringIndexer ++ Array(assembler,rf)
    val pipeline = new Pipeline().setStages(steps)


    // param grid search
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins,Array(100,500))
      .addGrid(rf.maxDepth,Array(6,10))
      .addGrid(rf.numTrees,Array(10,50))
      .addGrid(rf.impurity,Array("variance"))
      .build()

    //    val evaluator = new RegressionEvaluator()
    //      .setLabelCol("diff")
    //      .setPredictionCol("prediction")
    //      .setMetricName("r2")

    //select (prediction , true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("diff")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // 5 fold crossvalidation
    val crossvalidation = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val pipelineModel = crossvalidation.fit(trainingData)

    val featureImportances = pipelineModel.bestModel.asInstanceOf[PipelineModel]
      .stages.last.asInstanceOf[RandomForestRegressionModel].featureImportances.toArray

    assembler.getInputCols.zip(featureImportances).sortBy(_._2).foreach{case (feat,imp)=>
      println(s"feature: $feat, importance: $imp")}

    val bestEstimatorParaMap = pipelineModel
      .getEstimatorParamMaps
      .zip(pipelineModel.avgMetrics)
      .maxBy(_._2)
      ._1
    println(s"best params: \n $bestEstimatorParaMap")

    //make predictions
    val prediction = pipelineModel.transform(testData)

    //select example rows to display.
    prediction.printSchema()
    prediction.cache()
    prediction.show(5)
    prediction.createOrReplaceTempView("eta")
    val counttotal = prediction.count()
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
      s",trainingData: ${trainingData.count()} " +
      s",counttotal(testData): $counttotal")

    val rmse = evaluator.evaluate(prediction)
    println(s"rmse on test data: $rmse")

    // save model
    val modeldir = "/users/predictTimeArrivalRfModel"
    pipelineModel.write.overwrite().save(modeldir)

  }

}

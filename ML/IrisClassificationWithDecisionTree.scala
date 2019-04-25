package com.yd.spark.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
  * @ClassName: PredictWithDecisionTree
  * @Description:
  * @Usage: DecisionTree classification on iris data 
  * @Auther: suibianda LGD
  * @Date: 2019/4/25 下午4:33
  * @version : V1.0
  */
object IrisClassificationWithDecisionTree {

  case class Iris(sepalLength:Double, sepalWidth:Double, petalLength:Double, petalWidth:Double, category:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]").appName("IrisClassificationWithDecisionTree").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val file = "/Users/iris.data" 
    val data = spark.sparkContext.textFile(file).
      map{line=>
        val fields = line.split(",")
        val sl = fields(0).toDouble
        val sw = fields(1).toDouble
        val pl = fields(2).toDouble
        val pw = fields(3).toDouble
        val category = fields(4).split("-")(1)
        Iris(sl,sw,pl,pw,category)
      }.toDF()
    data.printSchema()
    data.createOrReplaceTempView("iriss")
    println("total records: "+data.count())
    println("train data first row: "+data.first())

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    println("total test records: "+testData.count())
    println("total train records: "+trainingData.count())

    println("what is the average sl,sw,pl,pw by category")
    spark.sql("SELECT category, avg(sepalLength) as avgsl, avg(sepalWidth) as avgsw, avg(petalLength) as avgpl, avg(petalWidth) as avgpw FROM iriss GROUP BY category").show

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureCols = Array("sepalLength", "sepalWidth", "petalLength", "petalWidth")
    //put features into a feature vector column
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val dTree = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setMaxBins(100)
    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    val steps = Array(labelIndexer, assembler, dTree, labelConverter)
    //    val steps = stringIndexers ++ Array(labeler, assembler, dTree)//验证feature importance 不需要one-hot-encoding

    val pipeline = new Pipeline().setStages(steps)

    val paramGrid = new ParamGridBuilder().addGrid(dTree.maxDepth, Array(2, 3, 4, 5)).build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel").setPredictionCol("prediction")
      .setMetricName("accuracy")

    // Set up 3-fold cross validation with paramGrid
    val crossval = new CrossValidator().setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(10)

//    val ntrain = trainingData.drop("delayed").drop("arrdelay")
//    println("ntrain.count： "+ntrain.count)
//    ntrain.show
    val cvModel = crossval.fit(trainingData)

// note if you want to do analyze the decision tree it is easier not to use OneHotEncoding
    val featureImportances = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[DecisionTreeClassificationModel].featureImportances.toArray

//    val treeModel = cvModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages.last.asInstanceOf[DecisionTreeClassificationModel]
//    val featureImportances = treeModel.featureImportances.toArray
//    treeModel.toDebugString
    val fis = s"features importances:\n ${featureCols.zip(featureImportances).map(t => s"\t${t._1} = ${t._2}").mkString("\n")}\n"
    println("featureImportances: "+featureImportances.toList)
    println(fis)

    val predictions = cvModel.transform(testData)

    val accuracy = evaluator.evaluate(predictions)

    val lp = predictions.select("indexedLabel", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter($"indexedLabel" === $"prediction").count()
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    println("ratio correct", ratioCorrect)
    println("ratio correct", accuracy)

    predictions.createOrReplaceTempView("iris")
    println("what is the average sl,sw,pl,pw by prediction")
    spark.sql("SELECT predictedLabel, avg(sepalLength) as avgsl, avg(sepalWidth) as avgsw, avg(petalLength) as avgpl, avg(petalWidth) as avgpw FROM iris GROUP BY predictedLabel").show
    predictions.show

    val modeldir = "/Users/xxxx"
    cvModel.write.overwrite().save(modeldir)

  }


}

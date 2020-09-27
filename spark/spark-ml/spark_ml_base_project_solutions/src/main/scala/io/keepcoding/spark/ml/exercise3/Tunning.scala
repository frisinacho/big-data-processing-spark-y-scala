package io.keepcoding.spark.ml.exercise3

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object Tunning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    val pokemonDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimitier", ",")
      .load("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-ml/spark_ml_base_project_solutions/src/main/resources/pokemon.csv")
      .cache()

    val Array(trainDF, testDF) = pokemonDF
      .select($"Type_1", $"Color", $"Height_m", $"Weight_kg", $"Catch_Rate", $"Body_Style")
      .randomSplit(Array(0.7, 0.3))

    val pipeline = Pipeline.load("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-ml/spark_ml_base_project_solutions/src/main/resources/pipeline")

    val (numTrees, maxDepth, maxBins, bootstrap) = pipeline.getStages.find(_.isInstanceOf[RandomForestClassifier]) match {
      case Some(stage) =>
        val rf = stage.asInstanceOf[RandomForestClassifier]
        (rf.numTrees, rf.maxDepth, rf.maxBins, rf.bootstrap)
      case None => throw new Exception("LogisticRegression is not a pipeline stage")
    }

    val paramGrid = new ParamGridBuilder()
      .addGrid(numTrees, Array(20, 30, 40))
      .addGrid(maxDepth, Array(5, 10, 15))
      .addGrid(maxBins, Array(32, 64))
      .addGrid(bootstrap, Array(true, false))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("classIndex")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2)

    val cvModel = cv.fit(trainDF)

    val prediction = cvModel.transform(testDF).cache()

    prediction
      .select("features", "prediction", "classIndex")
      .show(100, false)

    val total = prediction.count()
    val okCount = prediction
      .where($"prediction" === $"classIndex")
      .count()

    println(s"OK: ${okCount.toDouble / total * 100} %")


    spark.close()
  }
}

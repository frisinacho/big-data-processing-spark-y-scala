package io.keepcoding.spark.ml.exercise2

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

object LoadModel {
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


    val model = PipelineModel.load("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-ml/spark_ml_base_project_solutions/src/main/resources/model")

    // Probamos el modelo

    val prediction = model.transform(pokemonDF).cache()

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

package io.keepcoding.spark.ml.exercise1

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession

object PokemonClassifier {
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


    // Preparamos los datos, seleccionando las columnas que vamos a usar en nuestro modelo.

    val Array(trainDF, testDF) = pokemonDF
      .select($"Type_1", $"Color", $"Height_m", $"Weight_kg", $"Catch_Rate", $"Body_Style")
      .randomSplit(Array(0.7, 0.3))

    // Convertimos las columnas de texto en indices

    val classIndexer = new StringIndexer()
      .setInputCol("Type_1")
      .setOutputCol("classIndex")

    val colorIndexer = new StringIndexer()
      .setInputCol("Color")
      .setOutputCol("colorIndex")

    val bodyIndexer = new StringIndexer()
      .setInputCol("Body_Style")
      .setOutputCol("bodyIndex")


    // Creamos el vector de features (categorias)

    val categoryVect = new VectorAssembler()
      .setInputCols(Array("colorIndex", "bodyIndex"))
      .setOutputCol("catFeatures")

    val categoryIndex = new VectorIndexer()
      .setInputCol(categoryVect.getOutputCol)
      .setOutputCol("idxCatFeatures")

    // Creamos el vector de features (numericas) y normalizamos

    val numericVect = new VectorAssembler()
      .setInputCols(Array("Height_m", "Weight_kg", "Catch_Rate"))
      .setOutputCol("numFeatures")

    val minMaxFeatures = new MinMaxScaler()
      .setInputCol(numericVect.getOutputCol)
      .setOutputCol("normFeatures")

    // Combinamos las features numericas y de categoria

    val features = new VectorAssembler()
      .setInputCols(Array(categoryIndex.getOutputCol, minMaxFeatures.getOutputCol))
      .setOutputCol("features")

    // Creamos el modelo de LogisticRegression

    val rf = new RandomForestClassifier()
      .setLabelCol("classIndex")
      .setFeaturesCol("features")

    // Modelamos el pipeline, con todos sus stages

    val pipeline = new Pipeline()
      .setStages(Array(
        classIndexer, colorIndexer, bodyIndexer, categoryVect, categoryIndex, numericVect, minMaxFeatures,
        features, rf
      ))

    val model = pipeline.fit(trainDF)

    // Probamos el modelo

    val prediction = model.transform(testDF).cache()

    prediction
      .select("features", "prediction", "classIndex")
      .show(100, false)

    val total = prediction.count()
    val okCount = prediction
      .where($"prediction" === $"classIndex")
      .count()

    println(s"OK: ${okCount.toDouble / total * 100} %")

    pipeline.write.overwrite().save("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-ml/spark_ml_base_project_solutions/src/main/resources/pipeline")
    model.write.overwrite().save("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-ml/spark_ml_base_project_solutions/src/main/resources/model")

    spark.close()
  }
}

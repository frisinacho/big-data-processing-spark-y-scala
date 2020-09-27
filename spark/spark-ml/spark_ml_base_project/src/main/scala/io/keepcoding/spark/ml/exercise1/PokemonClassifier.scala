package io.keepcoding.spark.ml.exercise1

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
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

    // Preparamos los datos, seleccionando las columnas que vamos a usar en nuestro modelo.

    // Convertimos las columnas de texto en indices

    // Creamos el vector de features (categorias)

    // Creamos el vector de features (numericas) y normalizamos

    // Combinamos las features numericas y de categoria

    // Creamos el modelo de LogisticRegression

    // Modelamos el pipeline, con todos sus stages

    // Probamos el modelo


    spark.close()
  }
}

package io.keepcoding.spark.structured.streaming

import org.apache.spark.sql.SparkSession

object SparkStructuredStreamingBaseProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()
    // Podemos acceder al sparkContext desde el sparkSession
    // val sc = spark.sparkContext

    // Necesario importar los implicitos para ciertas conversiones
    // como RRDs to DataFrames
    import spark.implicits._

    // <code>

    // Debemos recordar cerrar la sesión y automáticamente cerrara el contexto
    spark.close()
  }

}

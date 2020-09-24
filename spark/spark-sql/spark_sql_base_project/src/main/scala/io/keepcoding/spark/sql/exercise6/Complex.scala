package io.keepcoding.spark.sql.exercise6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Complex {
  val dataPath = getClass.getClassLoader.getResource("exercise6/complex.json").getFile

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    // 1. Pintar el schema

    // <code>

    // 2. Calcular la media de humedad y temperatura por sensor y minuto

    // <code>


    spark.close()
  }

}

package io.keepcoding.spark.sql.exercise6

import org.apache.parquet.format.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}

object Arrays {
  val dataPath = getClass.getClassLoader.getResource("exercise6/arrays.json").getFile

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    //1. Pintar el schema
    val df = spark
      .read
      .format("json")
      .load(dataPath)
      .cache()

    df.printSchema()

    //2. Obtener el max y min de los elementos del array metrics
    df.select($"timestamp", $"sensor_id", $"locations", array_max($"metrics"), array_min($"metrics"))
      .show()

    //3. Obtener los datos pertenecientes a la ZonaA
    df.where(array_contains($"locations", "ZonaA"))
      .show()

    /*
        4. Si sabemos que:
        * La posicion 0 de metrics es igual a temperatura
        * La posicion 1 de metrics es igual a humedad
        * La posicion 0 de locations es zona
        * La posicion 1 de locations es pasillo
     */
    df.select($"timestamp", $"sensor_id",
      element_at($"locations", 1).as("zona"),
      element_at($"locations", 2).as("pasillo"),
      element_at($"metrics", 1).as("temperatura"),
      element_at($"metrics", 2).as("humedad")
    ).show

    spark.close()
  }

}

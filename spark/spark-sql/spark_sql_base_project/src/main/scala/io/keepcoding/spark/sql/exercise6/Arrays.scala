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

    // <code>

    //2. Obtener el max y min de los elementos del array metrics

    // <code>

    //3. Obtener los datos pertenecientes a la ZonaA

    // <code>

    /*
        4. Si sabemos que:
        * La posicion 0 de metrics es igual a temperatura
        * La posicion 1 de metrics es igual a humedad
        * La posicion 0 de locations es zona
        * La posicion 1 de locations es pasillo
     */

    // <code>

    spark.close()
  }

}

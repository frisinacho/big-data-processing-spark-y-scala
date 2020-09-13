package io.keepcoding.spark.sql.exercise4

import io.keepcoding.spark.sql.exercise3.ReadMoreDataSources.exercise2resultPath
import org.apache.spark.sql.{SaveMode, SparkSession}

object JDBC {
  val IpServer = ???

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    // 1. Muestra los resultados almacenados en la tabla `instituto`

    // 2. Une los resultados de la tabla con los json del ejercicio anterior
    //    vuelve a escribir los resultados en la tabla, sin generar duplicados.


    spark.close()
  }

}

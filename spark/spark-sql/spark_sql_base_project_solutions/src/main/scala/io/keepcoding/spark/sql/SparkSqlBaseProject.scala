package io.keepcoding.spark.sql
import org.apache.spark.sql.SparkSession

object SparkSqlBaseProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
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

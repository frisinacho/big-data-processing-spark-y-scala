package io.keepcoding.spark.sql.exercise2

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

object SparkSqlBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    val csvFilePath = getClass.getClassLoader.getResource("sample.csv").getFile
    val jsonFilePath = getClass.getClassLoader.getResource("sample.json").getFile

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 1. Leemos ambos datasources cada uno con su formato correspondiente.


    // 2. Unificamos los datos CSV & JSON, usando la función unionByName(...)

    // 3. ¿Qué alumno tiene mejor nota media del instituto?


    // 4. ¿Qué clase tiene las peores notas?

    // 5. Añade el tipo de curso a cada alumno 'ciencias'/'humanidades'
    //    dentro de la columna 'curso', escribe los resultados en formato JSON
    //    en un directorio temporal:
    //
    //    val outputDir = s"""${Files.createTempDirectory("spark").toFile.getAbsolutePath}/results"""

    val outputDir = s"""${Files.createTempDirectory("spark").toFile.getAbsolutePath}/results"""


    println(s"Result write into $outputDir")
    spark.close()
  }

}

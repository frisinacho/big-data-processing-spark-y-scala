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

    val csvDF = spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFilePath)

    val jsonDF = spark
      .read
      .format("json")
      .load(jsonFilePath)

    // 2. Unificamos los datos CSV & JSON, usando la función unionByName(...)
    val fullDF = jsonDF.unionByName(csvDF).cache()

    // 3. ¿Qué alumno tiene mejor nota media del instituto?
    fullDF
      .select($"nombre", $"apellido", $"nota")
      .groupBy($"nombre", $"apellido")
      .agg(avg($"nota").alias("nota_media"))
      .sort($"nota_media".desc)
      .limit(1)
      .show()

    // 4. ¿Qué clase tiene las peores notas?
    fullDF
      .select($"clase", $"nota")
      .groupBy($"clase")
      .agg(avg($"nota").as("nota_media"))
      .sort($"nota_media".asc)
      .limit(1)
      .show()

    // 5. Añade el tipo de curso a cada alumno 'ciencias'/'humanidades'
    //    dentro de la columna 'curso', escribe los resultados en formato JSON
    //    en un directorio temporal:
    //
    //    val outputDir = s"""${Files.createTempDirectory("spark").toFile.getAbsolutePath}/results"""
    val cursoDF = fullDF
      .select($"nombre", $"apellido", $"clase")
      .groupBy($"nombre", $"apellido")
      .agg(collect_list($"clase").as("clases"))
      .withColumn("curso",
        when(
          $"clases" === array(Seq("matematicas", "tecnologia", "ingles").map(lit): _*),
          "ciencias"
        ).otherwise("humanidades")
      )
      .select($"nombre", $"apellido", $"curso")

    val resultDF = fullDF.as("a")
      .join(cursoDF.as("b"), $"a.nombre" === $"b.nombre" && $"a.apellido" === $"b.apellido")
      .select((fullDF.columns.map(name => col(s"a.$name").as(name)) :+ col("b.curso").as("curso")): _*)
      .cache()

    val outputDir = s"""${Files.createTempDirectory("spark").toFile.getAbsolutePath}/results"""

    resultDF.show()
    resultDF
      .write
      .format("json")
      .save(outputDir)

    println(s"Result write into $outputDir")
    spark.close()
  }

}

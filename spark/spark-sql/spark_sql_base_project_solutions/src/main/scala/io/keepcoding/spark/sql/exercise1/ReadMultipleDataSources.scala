package io.keepcoding.spark.sql.exercise1

import org.apache.spark.sql.SparkSession

object ReadMultipleDataSources {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL KeepCoding Base")
      .getOrCreate()

    // Read CSV

    val csvFilePath = getClass.getClassLoader.getResource("sample.csv").getFile

    val csvDefault = spark
      .read
      .csv(csvFilePath)

    val csv = spark
      .read
      .format("csv")
      //.option("sep", ",")
      //.option("inferSchema", "true")
      //.option("header", "true")
      .load(csvFilePath)

    csvDefault.show
    csvDefault.printSchema

    csv.show
    csv.printSchema

    // Read JSON

    val jsonFilePath = getClass.getClassLoader.getResource("sample.json").getFile

    val jsonDefault = spark
      .read
      .json(jsonFilePath)

    val json = spark
      .read
      .format("json")
      .load(jsonFilePath)

    jsonDefault.show
    jsonDefault.printSchema

    json.show
    json.printSchema

    spark.close()
  }

}

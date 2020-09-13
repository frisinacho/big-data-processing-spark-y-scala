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

    // Read JSON

    spark.close()
  }

}

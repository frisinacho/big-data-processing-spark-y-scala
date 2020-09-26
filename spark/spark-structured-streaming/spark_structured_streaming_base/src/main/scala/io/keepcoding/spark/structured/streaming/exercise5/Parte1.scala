package io.keepcoding.spark.structured.streaming.exercise5

import org.apache.spark.sql.SparkSession

object Parte1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    // <code>

    spark.close()
  }

}

package io.keepcoding.spark.structured.streaming.exercise5

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Parte4 {


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

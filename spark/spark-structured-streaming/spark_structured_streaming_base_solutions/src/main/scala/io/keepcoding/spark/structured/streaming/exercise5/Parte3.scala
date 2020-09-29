package io.keepcoding.spark.structured.streaming.exercise5

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Parte3 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()


    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribePattern", "test")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .select(col("value").cast(StringType))
      .show(false)

    spark.close()
  }

}

package io.keepcoding.spark.structured.streaming.exercise5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Parte2 {

  def one(spark: SparkSession): DataFrame = ???

  def two(spark: SparkSession): DataFrame = ???

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()


    val kafkaDF = one(spark)
    //val kafkaDF = two(spark)

    kafkaDF.printSchema()

    kafkaDF.writeStream
      .format("console")
      .start()
      .awaitTermination()

    spark.close()
  }

}

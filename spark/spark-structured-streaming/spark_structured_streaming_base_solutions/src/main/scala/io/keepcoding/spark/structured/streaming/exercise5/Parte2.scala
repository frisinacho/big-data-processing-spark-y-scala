package io.keepcoding.spark.structured.streaming.exercise5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Parte2 {

  def one(spark: SparkSession): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .select(col("value").cast(StringType))
  }

  def two(spark: SparkSession): DataFrame = {

    val jsonSchema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .select(from_json(col("value").cast(StringType), jsonSchema).as("json"))
      .select("json.*")
  }

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

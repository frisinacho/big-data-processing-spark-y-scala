package io.keepcoding.spark.structured.streaming.exercise5

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Parte5 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

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
      .select(lit("out").as("topic"), col("id").as("key"), col("name").as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", Files.createTempDirectory("spark-").toAbsolutePath.toString)
      .trigger(Trigger.Continuous("10 seconds"))
      .start()
      .awaitTermination()

    spark.close()
  }

}

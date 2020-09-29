package io.keepcoding.spark.structured.streaming.exercise7

import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, from_json, lit, struct, to_json, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object SensorAggData {

  def readFromKafkaAndParser(spark: SparkSession, kafkaServer: String): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("sensor_id", IntegerType, nullable = false),
      StructField("temperature", IntegerType, nullable = false),
      StructField("humidity", IntegerType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false)
    ))

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", "sensor_data")
      .load()
      .select(from_json(col("value").cast(StringType), jsonSchema).as("json"))
      .select("json.*")
  }

  def aggregateData(parseDF: DataFrame): DataFrame = {
    import parseDF.sparkSession.implicits._

    parseDF
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"sensor_id", window($"timestamp", "1 minute"))
      .agg(avg($"temperature").as("temperature"), avg($"humidity").as("humidity"))
      .withColumn("timestamp", $"window.start".cast(LongType))
      .drop($"window")
  }

  def serializeAndWriteIntoKafka(aggDF: DataFrame, kafkaServer: String, checkpointPath: String): StreamingQuery = {
    import aggDF.sparkSession.implicits._

    aggDF
      .select(lit("sensor_agg_data").as("topic"), to_json(struct($"timestamp", $"sensor_id", $"temperature", $"humidity")).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("checkpointLocation", checkpointPath)
      .start()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[20]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    val parsedDF = readFromKafkaAndParser(spark, args(0))
    val aggDF = aggregateData(parsedDF)

    serializeAndWriteIntoKafka(aggDF, args(0), args(1))
      .awaitTermination()

    spark.close()
  }

}

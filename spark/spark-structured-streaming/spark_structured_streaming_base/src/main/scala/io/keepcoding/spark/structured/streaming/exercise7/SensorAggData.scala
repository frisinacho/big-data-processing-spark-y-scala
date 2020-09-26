package io.keepcoding.spark.structured.streaming.exercise7

import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, from_json, lit, struct, to_json, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object SensorAggData {

  def readFromKafkaAndParser(spark: SparkSession, kafkaServer: String): DataFrame = ???
  def aggregateData(parseDF: DataFrame): DataFrame = ???
  def serializeAndWriteIntoKafka(aggDF: DataFrame, kafkaServer: String, checkpointPath: String): StreamingQuery = ???

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

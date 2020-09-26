package io.keepcoding.spark.structured.streaming.exercise3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object JoinStreamBatch {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    val metadataSchema = StructType(Seq(
      StructField("sensor_id", IntegerType, nullable = false),
      StructField("location", StringType, nullable = false),
      StructField("maintainer", StringType, nullable = false)
    ))

    val metadataDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .schema(metadataSchema)
      .load("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise3/stream-batch/sensor_metadata.csv")

    val archivePath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise3/stream-batch/archive"
    val inputPath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise3/stream-batch/input"

    val schema = StructType(Seq(
      StructField("sensor_id", IntegerType, nullable = false),
      StructField("temperature", IntegerType, nullable = false),
      StructField("humidity", IntegerType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false)
    ))

    val sensorDF =
      spark
        .readStream
        .format("json")
        .option("cleanSource", "archive")
        .option("sourceArchiveDir", archivePath)
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .load(inputPath)

    val enrichDF = sensorDF.as("data")
      .join(
        metadataDF.as("metadata"),
        expr(""" data.sensor_id = metadata.sensor_id """.stripMargin),
        "left_outer"
      )
      .drop(col("metadata.sensor_id"))

    val query = enrichDF
      .writeStream
      .format("console")
      .start()

    query.awaitTermination()
    spark.close()
  }

}

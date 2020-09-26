package io.keepcoding.spark.structured.streaming.exercise2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object Window {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    val archivePath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise2/archive"
    val inputPath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise2/input"

    val schema = StructType(Seq(
      StructField("word", StringType, nullable = false)
    ))

    val inputDF = spark
      .readStream
      .format("text")
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", archivePath)
      .schema(schema)
      .load(inputPath)

    val transformDF = inputDF
      .withColumn("timestamp", unix_timestamp().cast(TimestampType))
      //.withWatermark("timestamp", "5 seconds")
      .groupBy($"word", window($"timestamp", "1 second"))
      .agg(count(col("*")).as("total_count"))
      .select($"word", $"window.start", $"total_count")

    val streamQuery = transformDF
      .writeStream
      .format("console")
      //.outputMode(OutputMode.Append())
      .outputMode(OutputMode.Complete())
      .start()

    println(s"STATUS: ${streamQuery.status}")

    streamQuery.awaitTermination()
    spark.close()
  }

}

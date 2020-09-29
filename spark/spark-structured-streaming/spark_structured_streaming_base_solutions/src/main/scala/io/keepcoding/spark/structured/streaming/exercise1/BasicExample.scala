package io.keepcoding.spark.structured.streaming.exercise1

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

object BasicExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    val archivePath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise1/archive"
    val inputPath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise1/input"

    val schema = StructType(Seq(
      StructField("sensor_id", IntegerType, nullable = false),
      StructField("temperature", IntegerType, nullable = false),
      StructField("humidity", IntegerType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    val inputDF = spark
      .readStream
      .format("json")
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", archivePath)
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(inputPath)

    //PARTE 1:

    //    val streamQuery = inputDF
    //      .writeStream
    //      .format("console")
    //      .start()

    //PARTE 2:

    //    val streamQuery = inputDF
    //      .select($"sensor_id", $"timestamp")
    //      .writeStream
    //      .format("console")
    //      .start()

    //PARTE 2:

    val streamQuery = inputDF
      .select($"sensor_id")
      .groupBy($"sensor_id")
      .agg(count("*"))
      .writeStream
      .format("console")
      .start()

    println(s"STATUS: ${streamQuery.status}")

    streamQuery.awaitTermination()
    spark.close()
  }

}

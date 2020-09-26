package io.keepcoding.spark.structured.streaming.exercise3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JoinStreamStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()

    import spark.implicits._

    val appSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("mac", StringType, nullable = false),
      StructField("app", StringType, nullable = false),
      StructField("bytes", IntegerType, nullable = false)
    ))

    val locSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("mac", StringType, nullable = false),
      StructField("location", StringType, nullable = false)
    ))

    val archivePath = "/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise3/stream-stream/archive"

    val appStream = spark
      .readStream
      .format("json")
      .schema(appSchema)
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", archivePath)
      .load("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise3/stream-stream/app-input")

    val locStream = spark
      .readStream
      .format("json")
      .schema(locSchema)
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", archivePath)
      .load("/Users/agf/Keepcoding/big-data-processing-spark-y-scala/spark/spark-structured-streaming/spark_structured_streaming_base_solutions/src/main/resources/exercise3/stream-stream/loc-input")

    val enrichStream = appStream.as("app").withWatermark("timestamp", "1 minute")
      .join(
        locStream.as("loc").withWatermark("timestamp", "1 minute"),
        expr(
          """
            | app.mac = loc.mac AND
            | app.timestamp <= loc.timestamp AND
            | app.timestamp >= loc.timestamp - interval 30 seconds
            |""".stripMargin
        )
      )

    val query = enrichStream
      .select(
        window($"app.timestamp", "1 minute").as("window"),
        col("app.app"),
        col("app.mac"),
        col("loc.location")
      )
      .groupBy($"window", $"app", $"location")
      .agg(approx_count_distinct($"mac").as("users"))
      .select($"window.start", $"app", $"location", $"users")
      .writeStream
      .format("console")
      .start()

    query.awaitTermination()
    spark.close()
  }

}

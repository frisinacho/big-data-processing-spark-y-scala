package io.keepcoding.spark.structured.streaming.exercise6

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Monitoring {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .config("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
      .getOrCreate()

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

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

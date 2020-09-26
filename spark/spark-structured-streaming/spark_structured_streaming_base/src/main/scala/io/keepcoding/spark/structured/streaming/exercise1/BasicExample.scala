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

    // <code>

    spark.close()
  }

}

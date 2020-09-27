package io.keepcoding.spark.ml.exercise2

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

object LoadModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Structured Streaming KeepCoding Base")
      .getOrCreate()


    spark.close()
  }
}

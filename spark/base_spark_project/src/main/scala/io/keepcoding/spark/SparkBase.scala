package io.keepcoding.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkBase {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[1]")
    val spark = new SparkContext(conf)
    // Spark Code
    spark.stop()
  }
}

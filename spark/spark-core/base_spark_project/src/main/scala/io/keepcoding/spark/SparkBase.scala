package io.keepcoding.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkBase {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // code

    sc.stop()
  }
}

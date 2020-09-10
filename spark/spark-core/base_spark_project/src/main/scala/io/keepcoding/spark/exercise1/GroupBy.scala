package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Agrupa los datos aplicando una función de agrupación.
object GroupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

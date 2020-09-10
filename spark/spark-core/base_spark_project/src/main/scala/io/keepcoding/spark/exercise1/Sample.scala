package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Muestra una fracción de los datos, con o sin replazo, utilizando una semilla que genera número aleatorio.
object Sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

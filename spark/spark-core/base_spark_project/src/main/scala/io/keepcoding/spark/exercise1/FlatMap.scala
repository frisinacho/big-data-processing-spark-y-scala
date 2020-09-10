package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Parecido a la operación map, pero la función devuelve una secuencia de valores.
object FlatMap {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

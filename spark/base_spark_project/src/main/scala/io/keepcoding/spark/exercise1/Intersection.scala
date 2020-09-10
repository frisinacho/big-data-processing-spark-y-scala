package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve los elementos de los RDDs que son iguales.
object Intersection {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

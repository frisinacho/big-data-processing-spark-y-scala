package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve un nuevo RDD con la unión de los elementos de los RDDs seleccionados.
object Union {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

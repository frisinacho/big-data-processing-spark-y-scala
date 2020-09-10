package io.keepcoding.spark.exercise2

import org.apache.spark.{SparkConf, SparkContext}

// Realiza una unión interna utilizando dos RDD de valor clave. Cuando se introduce conjuntos de datos de tipo
// (K, V) y (K, W), se devuelve un conjunto de datos de (K, (V, W)) para cada clave.
object Join {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

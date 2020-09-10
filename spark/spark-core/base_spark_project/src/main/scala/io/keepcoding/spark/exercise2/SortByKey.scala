package io.keepcoding.spark.exercise2

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve un conjunto de datos de pares (K, U) donde los valores de cada
// clave se agregan utilizando las funciones combinadas dadas y un valor por defecto de: "cero".
object SortByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

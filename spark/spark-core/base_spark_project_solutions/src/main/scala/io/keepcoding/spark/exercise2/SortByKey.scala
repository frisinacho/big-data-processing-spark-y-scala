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

    val dataRDD = sc.parallelize(Seq((2, "B"), (1, "A"), (3, "C")))
    val results = dataRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    val results1 = dataRDD.sortByKey(ascending = true).collect()

    println("---- SORT RESULT ----")
    results1.foreach(println)
    println("---------------------")

    sc.stop()
  }
}

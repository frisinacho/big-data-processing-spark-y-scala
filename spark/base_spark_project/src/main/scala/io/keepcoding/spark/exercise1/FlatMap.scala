package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Parecido a la operación map, pero la función devuelve una secuencia de valores.
object FlatMap {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq("Esto es una prueba"))
    val resultRDD = dataRDD.flatMap(_.split(" "))
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    val dataRDD1 = sc.parallelize(Seq(Seq(1, 1, 1), Seq(2, 2, 2), Seq(3, 3, 3)))
    val resultRDD1 = dataRDD1.map(seq => seq)
    val resultRDD2 = dataRDD1.flatMap(seq => seq)
    val results1 = resultRDD1.collect()
    val results2 = resultRDD2.collect()

    println("---- RESULT1 (map) ----")
    results1.foreach(println)
    println("-----------------------")

    println("---- RESULT2 (flatMap) ----")
    results2.foreach(println)
    println("---------------------------")

    sc.stop()
  }
}

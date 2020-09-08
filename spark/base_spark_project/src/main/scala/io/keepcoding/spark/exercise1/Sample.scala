package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Muestra una fracción de los datos, con o sin replazo, utilizando una semilla que genera número aleatorio.
object Sample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val resultRDD = dataRDD.sample(withReplacement = false, 0.5, 1)
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

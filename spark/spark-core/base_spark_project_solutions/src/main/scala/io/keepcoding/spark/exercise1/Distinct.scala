package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve los elementos de los RDDs que son distintos.
object Distinct {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(1, 2, 3, 4, 5, 2, 3, 4, 5))
    val resultRDD = dataRDD.distinct()
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve un nuevo RDD tras pasar cada elemento del RDD original a través de una función.
object Map {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val resultRDD = dataRDD.map(_ * 2)
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

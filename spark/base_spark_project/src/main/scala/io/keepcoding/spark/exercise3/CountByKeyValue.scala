package io.keepcoding.spark.exercise3

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve los elementos de los RDDs que son distintos.
object CountByKeyValue {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(("A", 1), ("A", 2), ("A", 1), ("B", 2)))
    val results = dataRDD.countByKey()

    println("---- RESULT KEY----")
    results.foreach(println)
    println("-------------------")

    val results1 = dataRDD.countByValue()

    println("---- RESULT VALUE ----")
    results1.foreach(println)
    println("----------------------")

    sc.stop()
  }
}

package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Agrupa los datos aplicando una función de agrupación.
object GroupBy {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    case class User(name: String, countryCode: String)
    val dataRDD = sc.parallelize(Seq(
      User("Andres", "ES"),
      User("Paco", "ES"),
      User("Bob", "UK"),
      User("John", "UK"),
      User("Joe", "US")
    ))

    val resultRDD = dataRDD.groupBy(_.countryCode)
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

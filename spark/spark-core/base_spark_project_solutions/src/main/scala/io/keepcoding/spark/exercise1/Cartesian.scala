package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Calcula el producto cartesiano entre dos RDD ,es decir cada elemento del primer RDD se une a cada elemento del segundo RDD, y los devuelve como un nuevo RDD.
object Cartesian {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(1, 2, 3, 4))
    val dataRDD1 = sc.parallelize(Seq("a", "b", "c", "d"))

    val resultRDD = dataRDD.cartesian(dataRDD1)
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

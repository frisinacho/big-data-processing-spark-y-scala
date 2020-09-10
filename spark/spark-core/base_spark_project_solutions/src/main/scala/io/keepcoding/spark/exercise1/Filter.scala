package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Realiza un filtrado de los elementos del RDD original para devolver un nuevo RDD con los datos filtrados.
object Filter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val resultRDD = dataRDD.filter(_ % 2 == 0)
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

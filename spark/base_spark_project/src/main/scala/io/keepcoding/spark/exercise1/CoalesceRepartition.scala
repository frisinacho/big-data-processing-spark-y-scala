package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Coalesce: Disminuye el número de particiones en el RDD al número especificado ( numPartitions)
// Repartition: Reorganiza aleatoriamente los datos en el RDD para crear más o menos particiones.
object CoalesceRepartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ), 5)
    val resultRDD = dataRDD.mapPartitionsWithIndex{(index, partition) => Iterator(s"$index -> ${partition.mkString(",")}")}
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    val resultRDD1 = dataRDD
      .coalesce(2)
      .mapPartitionsWithIndex{(index, partition) => Iterator(s"$index -> ${partition.mkString(",")}")}
    val results1 = resultRDD1.collect()

    println("---- RESULT COALESCE ----")
    results1.foreach(println)
    println("-------------------------")

    val resultRDD2 = dataRDD
      .repartition(10)
      .mapPartitionsWithIndex{(index, partition) => Iterator(s"$index -> ${partition.mkString(",")}")}
    val results2 = resultRDD2.collect()

    println("---- RESULT REPARTITION ----")
    results2.foreach(println)
    println("----------------------------")

    sc.stop()
  }
}

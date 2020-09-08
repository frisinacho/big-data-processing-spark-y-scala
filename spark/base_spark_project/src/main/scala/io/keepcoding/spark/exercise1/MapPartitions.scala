package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Similar a la operación map, pero se ejecuta por separado en cada partición del RDD.
object MapPartitions {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq("A", "B", "C", "D"), 2)
    val resultRDD = dataRDD.mapPartitions{partition => Iterator(partition.mkString(","))}
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    val resultRDD1 = dataRDD.mapPartitionsWithIndex{(index, partition) => Iterator(s"$index -> ${partition.mkString(",")}")}
    val results1 = resultRDD1.collect()

    println("---- RESULT1 ----")
    results1.foreach(println)
    println("-----------------")

    sc.stop()
  }
}

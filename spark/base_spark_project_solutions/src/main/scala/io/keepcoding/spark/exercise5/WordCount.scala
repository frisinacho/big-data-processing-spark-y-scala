package io.keepcoding.spark.exercise5

import io.keepcoding.spark.exercise4.ReadOperateWrite.getClass
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pathFilePath = getClass.getClassLoader.getResource("sample.txt").getFile
    val sampleRDD = sc.textFile(pathFilePath).cache()

    // Opción 1
    sampleRDD
      .flatMap(_.split("""\s+"""))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    // Opción 2
    sampleRDD
      .flatMap(_.split("""\s+"""))
      .map(word => (word, 1))
      .countByKey()
      .foreach(println)

    // Opción 3
    sampleRDD
      .flatMap(_.split("""\s+"""))
      .countByValue()
      .foreach(println)

    sc.stop()
  }
}

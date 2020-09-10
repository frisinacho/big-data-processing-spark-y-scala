package io.keepcoding.spark.exercise5

import io.keepcoding.spark.exercise5.WordCount.getClass
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pathFilePath = getClass.getClassLoader.getResource("sample.txt").getFile
    val sampleRDD = sc.textFile(pathFilePath)

    sampleRDD
      .flatMap(_.split("""\s+"""))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy({ case (_, count) => count }, ascending = false)
      .take(5)
      .foreach(println)

    sc.stop()
  }
}

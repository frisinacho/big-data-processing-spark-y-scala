package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Toma los datos RDD de cada partición y los envía a través de stdin a un shell-command
object Pipe {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

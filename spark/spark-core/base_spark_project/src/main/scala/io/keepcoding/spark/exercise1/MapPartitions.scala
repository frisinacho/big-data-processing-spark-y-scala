package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Similar a la operación map, pero se ejecuta por separado en cada partición del RDD.
object MapPartitions {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

package io.keepcoding.spark.exercise5

import io.keepcoding.spark.exercise4.ReadOperateWrite.getClass
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

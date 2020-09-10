package io.keepcoding.spark.exercise4

import java.io.File
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}

object ReadOperateWrite {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Toma los datos RDD de cada partición y los envía a través de stdin a un shell-command
object Pipe {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(
    """{"name":"sensor1", "status":"ON"}""",
    """{"name":"sensor2", "status":"OFF"}"""
    ))

    val resultRDD = dataRDD.pipe("/usr/local/bin/jq .name")
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

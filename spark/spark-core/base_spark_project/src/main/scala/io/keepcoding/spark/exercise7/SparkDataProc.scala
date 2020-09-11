package io.keepcoding.spark.exercise7

import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}

object SparkDataProc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      // No es necesario que indiquemos el master,
      // porque se indicara automamaticamente al subir la aplicación a DataProc.
      //.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD= sc.textFile(args(0)).cache()

    val uuid = UUID.randomUUID().toString
    println(s"[Driver] read data from ${args(0)}, write data into ${args(1)}$uuid")

    dataRDD
      .flatMap(_.split("""\s+"""))
      .map { word =>
        println(s"[Executor] Print $word!")
        (word, 1)
      }
      .reduceByKey(_ + _)
      .saveAsTextFile(s"${args(1)}$uuid")

    println(s"[Driver] Numero de lineas contadas: ${dataRDD.count()}")

    sc.stop()
  }
}

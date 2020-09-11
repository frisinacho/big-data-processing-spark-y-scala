package io.keepcoding.spark.exercise5

import io.circe.generic.auto._
import io.circe.parser._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JsonDataReadyForTesting {

  // Usarmeos la libreria https://circe.github.io/circe/
  // Para trabajar con los JSON en Scala
  sealed case class SensorData(sensor_id: Int, temperature: Int, humidity: Int, timestamp: Long)

  def processJsonData(data: RDD[String]): RDD[SensorData] = ???

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

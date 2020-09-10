package io.keepcoding.spark.exercise5

import io.circe.generic.auto._, io.circe.parser._
import org.apache.spark.{SparkConf, SparkContext}

object JsonData {

  // Usarmeos la libreria https://circe.github.io/circe/
  // Para trabajar con los JSON en Scala
  sealed case class SensorData(sensor_id: Int, temperature: Int, humidity: Int, timestamp: Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

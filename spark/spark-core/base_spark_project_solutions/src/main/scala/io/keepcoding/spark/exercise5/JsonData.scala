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
    val pathFilePath = getClass.getClassLoader.getResource("data.json").getFile

    sc.textFile(pathFilePath)
      // Decodificamos los JSON String en la case class SensorData
      // Devuelve un Either:
      //    Left -> Error
      //    Right -> SensorData
      .map(decode[SensorData])
      // Filtramos los eventos decodificados correctamente.
      .filter(_.isRight)
      .map(_.right.get)
      // Redondeamos los timestamp al minuto mas cercano
      // para poder agrupar por minuto.
      // https://www.unixtimestamp.com/index.php
      // Creamos un Key-Value Pair donde la clave es la tupla: minuto, sesnorId
      // Y el valor es la temperatura, humedad y añadimos un 1 para poder calcular el total.
      .map { case SensorData(sensor_id, temperature, humidity, timestamp) =>
        val roundTimestamp = (timestamp / 60).toInt * 60
        ((roundTimestamp, sensor_id), (temperature, humidity, 1))
      }
      // Reducimos los datos sumando las temperaturas, la humedad, y el total
      .reduceByKey {
        case (Tuple3(temp, humidiy, count), Tuple3(temp1, humidiy1, count1)) =>
          (temp + temp1, humidiy + humidiy1, count + count1)
      }
      // Calculamos las medias de las métricas y volvemos a construir un
      // objeto del tipo SensorData
      .map {
        case ((roundTimestamp, sensor_id), (sumTemp, sumHum, count)) =>
          SensorData(sensor_id, sumTemp / count, sumHum / count, roundTimestamp.toLong)
      }
      // Ordenamos los datos en base al timestamp y sensor_id
      // Obtenemos los datos y los imprimimos en pantalla
      .sortBy(sensor => (sensor.sensor_id, sensor.timestamp), ascending = true)
      // SPARK UI: .foreach{t => print("SLEEP --> "); Thread.sleep(10000); println(t)}
      .collect()
      .foreach(println)

    sc.stop()
  }
}

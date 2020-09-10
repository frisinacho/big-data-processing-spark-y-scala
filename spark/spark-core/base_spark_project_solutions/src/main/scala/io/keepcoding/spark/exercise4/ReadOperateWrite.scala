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

    // Obtener ficheros desde los resources.
    val pathFilePath = getClass.getClassLoader.getResource("sample.txt").getFile
    val outputFilePath = s"${new File(pathFilePath).getParent}/${UUID.randomUUID().toString}"

    // Optimización del procesamiento al tener dos acciones del mismo origen.
    // Acciones:
    //   1. Escribir las palabras en el fichero de salida: saveAsTextFile
    //   2. Contar todas las lineas: count
    // Podemos optimizar haciendo una cache del RDD, de esta forma unicamente se
    // leeran los datos del fichero una unica vez
    // val sampleRDD = sc.textFile(pathFileName).cache()

    val sampleRDD = sc.textFile(pathFilePath)
    sampleRDD
      .flatMap(_.split("""\s+"""))
      .filter(_.charAt(0).toUpper == 'T')
      .saveAsTextFile(outputFilePath)

    println(s"Resultados en: $outputFilePath")
    println(s"Numero de lineas: ${sampleRDD.count()}")

    sc.stop()
  }
}

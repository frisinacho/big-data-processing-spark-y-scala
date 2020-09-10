package io.keepcoding.spark.exercise3

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve los elementos de los RDDs que son distintos.
object ForeachPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      // Jugar a cambiar el numero de CPU,
      // para ver como las particiones
      // se envian de forma simultanea o no. --> local[*] VS local[1]
      .setMaster("local[1]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

package io.keepcoding.spark.exercise1

import org.apache.spark.{SparkConf, SparkContext}

// Coalesce: Disminuye el número de particiones en el RDD al número especificado ( numPartitions)
// Repartition: Reorganiza aleatoriamente los datos en el RDD para crear más o menos particiones.
object CoalesceRepartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

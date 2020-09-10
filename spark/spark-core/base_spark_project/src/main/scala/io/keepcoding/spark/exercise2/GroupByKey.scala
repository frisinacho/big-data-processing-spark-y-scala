package io.keepcoding.spark.exercise2

import org.apache.spark.{SparkConf, SparkContext}

// Similar al grupoBy, realiza el agrupamiento por clave de un conjunto de datos, pero en lugar de suministrar
// una función, el componente clave de cada par se presentará automáticamente al particionador.
object GroupByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // <code>

    sc.stop()
  }
}

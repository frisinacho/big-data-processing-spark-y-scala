package io.keepcoding.spark.exercise2

import org.apache.spark.{SparkConf, SparkContext}

// Similar al grupoBy, realiza el agrupamiento por clave de un conjunto de datos, pero en lugar de suministrar
// una función, el componente clave de cada par se presentará automáticamente al particionador.
object ReduceByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq((1, "A"), (1, "B"), (2, "C"), (2, "D"), (3, "E")))

    // val resultRDD = dataRDD.reduceByKey((str1, str2) => str1 + str2)
    val resultRDD = dataRDD.reduceByKey(_ + _)
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

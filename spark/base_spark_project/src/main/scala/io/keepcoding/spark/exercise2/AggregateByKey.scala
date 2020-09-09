package io.keepcoding.spark.exercise2

import org.apache.spark.{SparkConf, SparkContext}

// Devuelve un conjunto de datos de pares (K, U) donde los valores de cada
// clave se agregan utilizando las funciones combinadas dadas y un valor por defecto de: "cero".
object AggregateByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq(("A", "1"), ("A", 1), ("B", 2), ("B", "2"), ("C", "ABC"), ("C", 5), ("D", 10)))

    val resultRDD = dataRDD.aggregateByKey(100)(
      (zeroValue, value) =>  value match {
        case str: String if str forall Character.isDigit => str.toInt
        case int: Int => int
        case _ => zeroValue
      },
      _ + _
    )
    val results = resultRDD.collect()

    println("---- RESULT ----")
    results.foreach(println)
    println("----------------")

    sc.stop()
  }
}

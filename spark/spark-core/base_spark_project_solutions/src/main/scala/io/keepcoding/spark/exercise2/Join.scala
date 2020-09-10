package io.keepcoding.spark.exercise2

import org.apache.spark.{SparkConf, SparkContext}

// Realiza una unión interna utilizando dos RDD de valor clave. Cuando se introduce conjuntos de datos de tipo
// (K, V) y (K, W), se devuelve un conjunto de datos de (K, (V, W)) para cada clave.
object Join {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KeepcodingSparkBase")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Seq((1,"A"), (2, "B"), (3, "C"), (4, "D")))
    val dataRDD1 = sc.parallelize(Seq((1,"a"), (2, "b"), (3, "c"), (5, "D")))

    val resultRDD = dataRDD.join(dataRDD1)
    val results = resultRDD.collect()

    println("---- RESULT JOIN ----")
    results.foreach(println)
    println("---------------------")

    val resultRDD1 = dataRDD.leftOuterJoin(dataRDD1)
    val results1 = resultRDD1.collect()

    println("---- RESULT LEFTJOIN ----")
    results1.foreach(println)
    println("---------------------")

    val resultRDD2 = dataRDD.rightOuterJoin(dataRDD1)
    val results2 = resultRDD2.collect()

    println("---- RESULT RIGTHJOIN ----")
    results2.foreach(println)
    println("---------------------")

    val resultRDD3 = dataRDD.fullOuterJoin(dataRDD1)
    val results3 = resultRDD3.collect()

    println("---- RESULT FULLJOIN ----")
    results3.foreach(println)
    println("---------------------")

    sc.stop()
  }
}

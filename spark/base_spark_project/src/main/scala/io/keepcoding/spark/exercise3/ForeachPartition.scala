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

    val dataRDD = sc.parallelize(Seq(1, 2, 3, 4, 5, 1, 2, 3, 4, 5), 4)
    dataRDD.foreachPartition{ partition =>
      // Este código se ejecuta en cada partición.
      // Ideal para conexiones con BBDD, enviar datos a servicios externos.
      // Aquí se inicializada la conexión
      println("INICIANDO CONEXION EN LA PARTICION")

      partition.foreach{ element =>
      // Cada elemento del dataset.
      // Se utiliza la conexión inicializada para enviar los datos.
      println(s"ENVIANDO EL DATO: $element")
    }}

    sc.stop()
  }
}

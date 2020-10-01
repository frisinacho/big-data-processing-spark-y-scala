package io.keepcoding.spark.graphx.exercise1

import scala.util.Random

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BasicExample {
  trait User {
    val id: VertexId = Random.nextLong()

    def toTuple: (VertexId, User) = (id, this)
  }

  case class Professor(name: String, course: String) extends User
  case class Alumn(name: String, age: Int) extends User

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 1. Crea un grafo con estos usuarios y sus relaciones.

    // 2. Imprime todos los elementos del grafo, con sus relaciones.

    // 3. Cuenta el numero de profesor de nuestro sistema.

    // 4. Cuantos alumnos tiene actualmente el profesor Pepe.

    // 5. Cual es el alumno más mayor de Pepe.

    // 6. Elimina las relaciones de compañeros, creando un nuevo grafo

    // 7. Si consideramos que un persona tiene que estar más valorada en base al numero de relaciones directas o indirectas
    //    cual sería la clasificación, podemos utilizar un algoritmo de PageRank.

    // 8. Calcula el numero de relaciones que forman un triangualo en nuestro colegio.

    spark.stop()
  }
}

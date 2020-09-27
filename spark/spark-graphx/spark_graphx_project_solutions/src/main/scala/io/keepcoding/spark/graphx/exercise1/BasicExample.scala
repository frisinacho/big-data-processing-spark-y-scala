package io.keepcoding.spark.graphx.exercise1

import scala.util.Random

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BasicExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 1. Crea un grafo con estos usuarios y sus relaciones.

    trait User {
      val id: VertexId = Random.nextLong()

      def toTuple: (VertexId, User) = (id, this)
    }

    case class Professor(name: String, course: String) extends User
    case class Alumn(name: String, age: Int) extends User

    val pepe = Professor("pepe", "maths")
    val rocio = Professor("rocio", "spanish")
    val sara = Alumn("sara", 20)
    val luis = Alumn("luis", 21)
    val fede = Alumn("fede", 18)
    val maria = Alumn("maria", 22)

    val persons = Seq(pepe, rocio, sara, luis, fede, maria).map(_.toTuple)
    val personsRDD: RDD[(VertexId, User)] = sc.parallelize(persons)

    val relations = Seq(
      Edge(pepe.id, sara.id, "professor"),
      Edge(pepe.id, luis.id, "professor"),
      Edge(pepe.id, fede.id, "professor"),
      Edge(pepe.id, maria.id, "professor"),

      Edge(rocio.id, sara.id, "professor"),
      Edge(rocio.id, fede.id, "professor"),

      Edge(sara.id, fede.id, "friend"),
      Edge(sara.id, maria.id, "friend"),
      Edge(luis.id, fede.id, "friend"),

      Edge(pepe.id, rocio.id, "workmate")
    )

    val relationRDD: RDD[Edge[String]] = sc.parallelize(relations)

    val graph: Graph[User, String] = Graph(personsRDD, relationRDD)

    // 2. Imprime todos los elementos del grafo, con sus relaciones.
    graph
      .triplets
      .collect()
      .foreach { triplet =>
        println(s"${triplet.srcAttr} is ${triplet.attr} of ${triplet.dstAttr}")
      }

    // 3. Cuenta el numero de profesor de nuestro sistema.

    val professorNum = graph.vertices.filter { case (_, user) => user match {
      case _: Professor => true
      case _ => false
    }
    }.count()

    println(s"El numero de profesores es ${professorNum}")

    // 4. Cuantos alumnos tiene actualmente el profesor Pepe.
    val alumnsProfessorPepe = graph.triplets.filter(triplets => (triplets.srcAttr, triplets.attr) match {
      case (Professor("pepe", _), "professor") => true
      case _ => false
    }).count()

    println(s"El profesor Pepe tiene ${alumnsProfessorPepe} alumnos")

    // 5. Cual es el alumno más mayor de Pepe.

    val oldestAlumn = graph.triplets
      .filter { triplet =>
        (triplet.srcAttr, triplet.attr) match {
          case (Professor("pepe", _), "professor") => true
          case _ => false
        }
      }
      .map(_.dstAttr)
      .reduce { case (a1@Alumn(_, edad1), a2@Alumn(_, edad2)) =>
        if (edad1 > edad2) a1 else a2
      }

    println(s"El alumno más mayor del profesor Pepe es ${oldestAlumn}")

    // 6. Elimina las relaciones de compañeros, creando un nuevo grafo

    val newGraph = graph.subgraph(epred => epred.attr != "workmate")

    newGraph
      .triplets
      .collect()
      .foreach { triplet =>
        println(s"${triplet.srcAttr} is ${triplet.attr} of ${triplet.dstAttr}")
      }

    // 7. Si consideramos que un persona tiene que estar más valorada en base al numero de relaciones directas o indirectas
    //    cual sería la clasificación, podemos utilizar un algoritmo de PageRank.

    val ranks = graph.pageRank(0.0001).vertices

    val personRanking = personsRDD
      .join(ranks)
      .map { case (_, (person, rank)) => (person, rank) }
      .sortBy(_._2, ascending = false)
      .collect()

    println("People Rank:")
    personRanking
      .foreach(println)

    // 8. Calcula el numero de relaciones que forman un triangualo en nuestro colegio.
    val triangleCount = graph
      .triangleCount
      .vertices
      .join(personsRDD)
      .map { case (_, (count, person)) => (person, count) }
      .sortBy(_._2, ascending = false)
      .collect()

    println("Triangle Count:")
    triangleCount
      .foreach(println)

    spark.stop()
  }
}

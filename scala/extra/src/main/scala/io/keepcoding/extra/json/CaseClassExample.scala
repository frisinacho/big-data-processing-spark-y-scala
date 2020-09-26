package io.keepcoding.extra.json

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

object CaseClassExample {
  def main(args: Array[String]): Unit = {
    case class User(name: String, age: Int)

    println(User("Pepe", 28).asJson.noSpaces)

    val jsonRaw = """{"name":"Pepe","age":28}""".stripMargin

    decode[User](jsonRaw) match {
      case Left(error) => println(s"Error: ${error.getMessage}")
      case Right(user) => println(s"La edad es: ${user.age}")
    }

    val jsonError = """{"name:"Pepe","age":28}""".stripMargin

    decode[User](jsonError) match {
      case Left(error) => println(s"Error: ${error.getMessage}")
      case Right(user) => println(s"La edad es: ${user.age}")
    }
  }
}

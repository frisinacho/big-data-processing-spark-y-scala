package io.keepcoding.extra.json

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

object JsonObjectExample {

  def main(args: Array[String]): Unit = {
    val jsonRaw = """{"name":"Pepe","age":28}""".stripMargin

    parse(jsonRaw) match {
      case Right(json) => println(json.deepMerge(Map("test" -> "123").asJson))
      case Left(error) => println(s"Error: ${error.getMessage}")
    }

  }
}

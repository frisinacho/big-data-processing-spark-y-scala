package io.keepcoding.extra.http

import scala.io.Source

import scalaj.http._
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._

object HttpClientExample {
  def main(args: Array[String]): Unit = {

    // STRING RESPONSE
    val response: HttpResponse[String] = Http("https://jsonplaceholder.typicode.com/todos/1").asString
    println(s"STRING RESPONSE: ${response.body}")

    // JSON RESPONSE DECODE
    case class Message(userId: Int, id: Int, title: String, completed: Boolean)

    val responseCase: HttpResponse[Either[Error, Message]] =
      Http("https://jsonplaceholder.typicode.com/todos/1")
        .execute(parser = { inputStream =>
          decode[Message](Source.fromInputStream(inputStream).mkString)
        })

    responseCase.body match {
      case Right(message) =>
        println(s"The title is ${message.title}")
      case Left(error) =>
        println(error)
    }
  }
}
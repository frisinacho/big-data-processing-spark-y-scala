package io.keepcoding.spark.sql.exercise4

import java.sql.{Connection, DriverManager}

import io.keepcoding.spark.sql.exercise3.ReadMoreDataSources.getClass

object ProvisionPostgresql {
  val IpServer = "34.71.29.40"
  val exercise2resultPath = getClass.getClassLoader.getResource("exercise2_output").getFile

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "keepcoding"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")
      println("Creando la tabla instituto(nombre TEXT, apellido TEXT, curso TEXT, clase TEXT, nota BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS instituto(nombre TEXT, apellido TEXT, curso TEXT, clase TEXT, nota BIGINT)")

      println("Dando de alta nuevo alumno Federico Gonzalez, que pertenece a idiomas(italiano:8, ingles:7, frances:9)")
      statement.execute("INSERT INTO instituto (nombre, apellido, curso, clase, nota) VALUES ('federico', 'gonzalez', 'idiomas', 'italiano', 8)")
      statement.execute("INSERT INTO instituto (nombre, apellido, curso, clase, nota) VALUES ('federico', 'gonzalez', 'idiomas', 'ingles', 7)")
      statement.execute("INSERT INTO instituto (nombre, apellido, curso, clase, nota) VALUES ('federico', 'gonzalez', 'idiomas', 'frances', 9)")

    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}

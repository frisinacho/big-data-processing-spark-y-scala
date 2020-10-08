package io.keepcoding.data.simulator.provisioner

import java.sql.Connection

object Provisioner {
  def main(args: Array[String]) {
    val IpServer = "34.78.249.75"

    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "postgres"

    // there's probably a better way to do this
    var connection: Connection = null
  }
}

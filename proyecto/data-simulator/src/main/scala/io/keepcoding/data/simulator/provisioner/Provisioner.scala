package io.keepcoding.data.simulator.provisioner

import java.sql.{Connection, DriverManager}

object Provisioner {
  def main(args: Array[String]) {
    val IpServer = "34.78.249.75"

    // connect to the postgres database
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
      println("Connection established successfully!")

      println("Creating the user_metadata table (id TEXT, name TEXT, email TEXT, quota BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS user_metadata(id TEXT, name TEXT, email TEXT, quota BIGINT)")

      println("Initial load for data.")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")

      println("Creating the bytes_by_antenna_agg table (antenna_id TEXT, date TIMESTAMP, sum_bytes_antenna BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_by_antenna_agg(antenna_id TEXT, date TIMESTAMP, sum_bytes_antenna BIGINT)")

      println("Creating the bytes_by_user_agg table (id TEXT, date TIMESTAMP, sum_bytes_user BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_by_user_agg(id TEXT, date TIMESTAMP, sum_bytes_user BIGINT)")

      println("Creating the bytes_by_app_agg table (app TEXT, date TIMESTAMP, sum_bytes_user BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_by_app_agg(app TEXT, date TIMESTAMP, sum_bytes_app BIGINT)")

      println("Creating the batch_bytes_by_antenna_agg table (antenna_id TEXT, sum_bytes_antenna BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS batch_bytes_by_antenna_agg(antenna_id TEXT, sum_bytes_antenna BIGINT)")

      println("Creating the batch_bytes_by_user_agg table (email TEXT, sum_bytes_user BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS batch_bytes_by_user_agg(email TEXT, sum_bytes_user BIGINT)")

      println("Creating the batch_bytes_by_app_agg table (app TEXT, sum_bytes_user BIGINT).")
      statement.execute("CREATE TABLE IF NOT EXISTS batch_bytes_by_app_agg(app TEXT, sum_bytes_app BIGINT)")

      println("Creating the batch_over_quota_agg table (email TEXT).")
      statement.execute("CREATE TABLE IF NOT EXISTS batch_over_quota_agg(email TEXT)")
    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }
}

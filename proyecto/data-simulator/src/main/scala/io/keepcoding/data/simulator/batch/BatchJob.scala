package io.keepcoding.data.simulator.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, year: String, month: String, day: String, hour: String): DataFrame

  def computeBytesReceivedByAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedByUser(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedByApp(dataFrame: DataFrame): DataFrame
  def computeUsersOverQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(year, month, day, hour, storagePath, jdbcUri, jdbcUser, jdbcPassword, aggJdbcAntennaTable, aggJdbcUserTable, aggJdbcAppTable, aggJdbcQuotaTable) = args
    println(s"Running with: ${args.toSeq}")

    val devicesDF = readFromStorage(storagePath, year, month, day, hour)

    val aggByAntennaDF = computeBytesReceivedByAntenna(devicesDF)
    val aggByUserDF = computeBytesTransmittedByUser(devicesDF)
    val aggByAppDF = computeBytesTransmittedByApp(devicesDF)
    val aggByQuotaDF = computeUsersOverQuota(devicesDF)

    writeToJdbc(aggByAntennaDF, jdbcUri, aggJdbcAntennaTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByUserDF, jdbcUri, aggJdbcUserTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByAppDF, jdbcUri, aggJdbcAppTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByQuotaDF, jdbcUri, aggJdbcQuotaTable, jdbcUser, jdbcPassword)

    spark.close()

    // ARGS: <YEAR> <MONTH> <DAY> <HOUR> /tmp/data-simulator jdbc:postgresql://34.78.249.75:5432/postgres postgres keepcoding batch_bytes_by_antenna_agg batch_bytes_by_user_agg batch_bytes_by_app_agg batch_over_quota_agg
  }
}

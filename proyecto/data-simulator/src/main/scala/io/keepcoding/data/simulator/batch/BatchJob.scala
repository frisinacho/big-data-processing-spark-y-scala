package io.keepcoding.data.simulator.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, year: String, month: String, day: String, hour: String): DataFrame

  def computeBytesReceivedByAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedByUser(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedByApp(dataFrame: DataFrame): DataFrame
  def computeUsersOverQuota(dataFrame: DataFrame): DataFrame

  def run(args: Array[String]): Unit = {
    val Array(year, month, day, hour, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val devicesDF = readFromStorage(storagePath, year, month, day, hour)

    val aggByAntennaDF = computeBytesReceivedByAntenna(devicesDF)
    val aggByUserDF = computeBytesTransmittedByUser(devicesDF)
    val aggByAppDF = computeBytesTransmittedByApp(devicesDF)

    devicesDF.show()

    aggByAntennaDF.show()
    aggByUserDF.show()
    aggByAppDF.show()

    spark.close()

    // ARGS: year month day hour /tmp/data-simulator/
  }
}

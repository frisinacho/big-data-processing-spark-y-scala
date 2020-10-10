package io.keepcoding.data.simulator.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, year: String, month: String, day: String, hour: String): DataFrame

  def run(args: Array[String]): Unit = {
    val Array(year, month, day, hour, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val devicesDF = readFromStorage(storagePath, year, month, day, hour)

    devicesDF.show()

    spark.close()

    // ARGS: year month day hour /tmp/data-simulator/
  }
}

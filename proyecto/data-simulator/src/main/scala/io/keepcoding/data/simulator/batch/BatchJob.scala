package io.keepcoding.data.simulator.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val devicesDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))

    devicesDF.show()

    spark.close()

    // ARGS: <filterDate> /tmp/data-simulator/
  }
}

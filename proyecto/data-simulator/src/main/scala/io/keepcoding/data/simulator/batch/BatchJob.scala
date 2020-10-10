package io.keepcoding.data.simulator.batch

import org.apache.spark.sql.SparkSession

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait BatchJob {

  val spark: SparkSession

}

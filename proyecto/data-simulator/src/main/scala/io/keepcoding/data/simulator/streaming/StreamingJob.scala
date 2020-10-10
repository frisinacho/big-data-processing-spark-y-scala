package io.keepcoding.data.simulator.streaming
import io.keepcoding.data.simulator.streaming.StreamingJobImpl._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDevicesWithUserMetadata(devicesDF: DataFrame, userMetadataDF: DataFrame): DataFrame

  def computeBytesReceivedByAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedByUser(dataFrame: DataFrame): DataFrame
  def computeBytesTransmittedByApp(dataFrame: DataFrame): DataFrame

    def run(args: Array[String]): Unit = {
      val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
      println(s"Running with: ${args.toSeq}")

      val kafkaDF = readFromKafka(kafkaServer, topic)
      val devicesDF = parserJsonData(kafkaDF)
      val userMetadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
      val devicesMetadataDF = enrichDevicesWithUserMetadata(devicesDF, userMetadataDF)
      val storageFuture = writeToStorage(devicesMetadataDF, storagePath)
      val aggByAntennaDF = computeBytesReceivedByAntenna(devicesMetadataDF)
      val aggFuture = writeToJdbc(aggByAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

      Await.result(Future.sequence(Seq(storageFuture, aggFuture)), Duration.Inf)



      spark.close()

      // ARGS: KAFKA_SERVER:9092 devices jdbc:postgresql://34.78.249.75:5432/postgres bytes_by_antenna_agg user_metadata postgres keepcoding gs://keepcoding_nacho/data-simulator/
    }

}

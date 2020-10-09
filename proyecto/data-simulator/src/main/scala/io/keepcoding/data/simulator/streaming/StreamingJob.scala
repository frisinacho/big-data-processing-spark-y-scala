package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DevicesMessage(timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDevicesWithUserMetadata(devicesDF: DataFrame, userMetadataDF: DataFrame): DataFrame

    def run(args: Array[String]): Unit = {
      val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword) = args
      println(s"Running with: ${args.toSeq}")

      val kafkaDF = readFromKafka(kafkaServer, topic)
      val devicesDF = parserJsonData(kafkaDF)
      val userMetadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
      val devicesMetadataDF = enrichDevicesWithUserMetadata(devicesDF, userMetadataDF)

      devicesMetadataDF
        .writeStream
        .format("console")
        .start()
        .awaitTermination()



      spark.close()

      // ARGS: KAFKA_SERVER:9092 devices jdbc:postgresql://34.78.249.75:5432/postgres user_metadata postgres keepcoding
    }

}

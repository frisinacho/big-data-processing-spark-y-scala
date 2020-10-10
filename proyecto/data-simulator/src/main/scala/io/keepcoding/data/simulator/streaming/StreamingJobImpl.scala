package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, dayofmonth, from_json, hour, month, year}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val schema: StructType = ScalaReflection.schemaFor[DevicesMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json(col("value").cast(StringType), schema).as("json"))
      .select("json.*")
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDevicesWithUserMetadata(devicesDF: DataFrame, userMetadataDF: DataFrame): DataFrame = {
    devicesDF.as("device")
      .join(
        userMetadataDF.as("userMetadata"),
        $"device.id" === $"userMetadata.id"
      ).drop($"userMetadata.id")
  }

   def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year(($"timestamp").cast(TimestampType)).as("year"),
        month(($"timestamp").cast(TimestampType)).as("month"),
        dayofmonth(($"timestamp").cast(TimestampType)).as("day"),
        hour(($"timestamp").cast(TimestampType)).as("hour")
      )
     // I had to cast the "timestamp" column as TimestampType, because it was stored as BIGINT on the database.

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)
}

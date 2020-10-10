package io.keepcoding.data.simulator.batch
import org.apache.spark.sql.functions.{sum, when}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, year: String, month: String, day: String, hour: String): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === year &&
          $"month" === month &&
          $"day" === day &&
          $"hour" === hour
      )
  }

  override def computeBytesReceivedByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(($"timestamp").cast(TimestampType), $"bytes", $"antenna_id")
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"antenna_id")
      .agg(sum($"bytes").as("sum_bytes_antenna"))
      .select($"antenna_id", $"sum_bytes_antenna")
  }

  override def computeBytesTransmittedByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(($"timestamp").cast(TimestampType), $"bytes", $"email")
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"email")
      .agg(sum($"bytes").as("sum_bytes_user"))
      .select($"email", $"sum_bytes_user")
  }

  override def computeBytesTransmittedByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(($"timestamp").cast(TimestampType), $"bytes", $"app")
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"app")
      .agg(sum($"bytes").as("sum_bytes_app"))
      .select($"app", $"sum_bytes_app")
  }

  override def computeUsersOverQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(($"timestamp").cast(TimestampType), $"bytes", $"email", $"quota")
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"email", $"quota")
      .agg(sum($"bytes").as("sum_bytes_user"))
      //.withColumn("over_quota", when($"sum_bytes_user".gt($"quota"), "yes").otherwise("no"))
      .filter("sum_bytes_user > quota")
      .select($"email")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  def main(args: Array[String]): Unit = run(args)
}

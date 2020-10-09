package io.keepcoding.data.simulator.streaming
import org.apache.spark.sql.SparkSession

trait StreamingJob {

    val spark: SparkSession

    def run(): Unit = {
      println("Hello world!")
    }

}

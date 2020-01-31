package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

object KafkaCheckApp extends StrictLogging with SetupSpark  {

  override protected val appName: String = "Kafka Integration Test"
  override protected val spark: SparkSession = setupSpark()

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    val source = new KafkaSource()
    val sDF = source.read()

    val sink = new ConsoleSink()
    sink.init()
    sink.write(sDF)

  }

}

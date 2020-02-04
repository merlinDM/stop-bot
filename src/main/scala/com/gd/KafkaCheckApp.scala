package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

object KafkaCheckApp extends StrictLogging with SetupSpark  {

  override protected val appName: String = "Kafka Integration Test"
  override protected val spark: SparkSession = setupSpark()

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    val sourceConfiguration = KafkaSourceConfiguration(
      retryInterval = 20
    )
    val source = new KafkaSource(sourceConfiguration)
    val sDF = source.read()

    val consoleSourceConfiguration = ConsoleSourceConfiguration(timeoutMs = None)
    val sink = new ConsoleSink(consoleSourceConfiguration)
    sink.write(sDF)

  }

}

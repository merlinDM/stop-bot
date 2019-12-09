package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

object StopBotApp extends StrictLogging with SetupSpark {

  override protected val appName: String = "Stop Bot App"
  override protected val spark: SparkSession = setupSpark()

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    // Load Source properties
    val source = new KafkaSource()
    source.init()
    val sDF = source.read()

    val sink = new IgniteSink()
    sink.init(timeoutMs = None, configFileLocation = "ignite-client-config.xml")
    sink.write(sDF)

  }

}

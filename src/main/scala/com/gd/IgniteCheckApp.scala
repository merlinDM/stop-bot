package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

object IgniteCheckApp extends StrictLogging {

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    setupSpark()

    // Load Source properties
    val source = new JsonFileSource()
    source.init(datadir = "/shared/data/")
    val sDF = source.read()

    val sink = new IgniteSink()
    sink.init(configFileLocation = "ignite-client-config.xml")
    sink.write(sDF)

  }

  private def setupSpark(): Unit = {
    // Set up SparkStreaming
    SparkSession
      .builder()
      .appName("Stop Bot")
      .getOrCreate()
  }

}

package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

object KafkaCheckApp extends StrictLogging {

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    setupSpark()

    val source = new KafkaSource()
    source.init()
    val sDF = source.read()

    val sink = new ConsoleSink()
    sink.init()
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

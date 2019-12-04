package com.gd

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.StrictLogging

object StopBotApp extends StrictLogging {

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    setupSpark()

    // Load Source properties
    val source = new KafkaSource()
    source.init()
    val sDF = source.read()

    val stopBotTransform = new StopBotTransform()
    val tDF = stopBotTransform.transform(sDF)

    val sink = new IgniteSink()
    sink.init()
    sink.write(tDF)

  }

  private def setupSpark(): Unit = {
    // Set up SparkStreaming
    SparkSession
      .builder()
      .appName("Stop Bot")
      .getOrCreate()
  }

}

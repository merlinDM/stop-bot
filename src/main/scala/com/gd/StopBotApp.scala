package com.gd

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.streaming.Trigger

object StopBotApp extends StrictLogging {

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    setupSpark()

    // Load Source properties
    val source = new KafkaSource()
    source.init()
    val sdf = source.read()

    val sink = new ConsoleSink()
    sink.init()
    sink.write(sdf)

    // Load Sink properties

    // Define Transformations

    // Start Application

    // * Define what to do on shutdown

    // * Define what to do on restart

  }

  def setupSpark(): Unit = {
    // Set up SparkStreaming
    val spark = SparkSession
      .builder()
      .appName("Stop Bot")
      .getOrCreate()
  }

}

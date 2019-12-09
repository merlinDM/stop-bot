package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

object IgniteCheckApp extends StrictLogging with SetupSpark  {

  override protected val appName: String = "Ignite Integration Test"
  override protected val spark: SparkSession = setupSpark()

  def main(args: Array[String]): Unit = {
    // Set up Logging
    logger.info("Starting the application")

    // Load Source properties
    val source = new JsonFileSource()
    source.init(datadir = "/shared/data/")
    val sDF = source.read()

    val sink = new IgniteSink()
    sink.init(configFileLocation = "ignite-client-config.xml")
    sink.write(sDF)

  }

}

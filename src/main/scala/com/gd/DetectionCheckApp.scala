package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.sql.SparkSession

object DetectionCheckApp extends StrictLogging with SetupSpark {

  override protected val appName: String = "Ignite Integration Test"
  override protected val spark: SparkSession = setupSpark()

  def main(args: Array[String]): Unit = {

    LogManager.getLogger("org.apache.spark").setLevel(Level.OFF)

    val source = new JsonFileSource()
    source.init(datadir = "build/docker/data")
    val sDF = source.read()

    val detector = new BotDetector()
    val tDF = detector.aggregate(sDF)

    val consoleSourceConfiguration = ConsoleSourceConfiguration(timeoutMs = None)
    val sink = new ConsoleSink(consoleSourceConfiguration)
    sink.write(tDF)

  }
}

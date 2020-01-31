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

    val transform = new BotDetector()
    val tDF = transform.transform(sDF)

    val sink = new ConsoleSink()
    sink.init(maybeTimeoutMs = None)
    sink.write(tDF)

  }
}

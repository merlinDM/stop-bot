package com.gd

import com.gd.model.IpfixResult
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object StopBotApp extends StrictLogging with SetupSpark {

  override protected val appName: String = "Stop Bot App"
  override protected val spark: SparkSession = setupSpark()

  def main(args: Array[String]): Unit = {
    logger.info("Starting the application")

    val kafkaConfig = KafkaSourceConfiguration(topic = "test")
    val source = new KafkaSource(kafkaConfig)
    val logs = source.read()

    val detector = new BotDetector()
    val detectedBots = detector.aggregate(logs)

    val igniteSinkConfig = IgniteSourceConfiguration(
      timeoutMs = None,
      configFile = "ignite-client-config.xml",
      instanceName = "df-writer-client"
    )
    val igniteSink = new IgniteSource(igniteSinkConfig)
    igniteSink.write(detectedBots)

    val igniteSourceConfig = IgniteSourceConfiguration(
      timeoutMs = None,
      configFile = "ignite-client-config.xml",
      instanceName = "df-reader-client"
    )
    val igniteSource = new IgniteSource(igniteSourceConfig)
    val allBots = igniteSource.read()

    val logsWithBots = detector.join(logs, allBots)

    val sinkCfg = CassandraSourceConfiguration(keyspace = "stopbot", table = "access_log", mode = SaveMode.Append)
    val cassandraSink = new CassandraSource[IpfixResult](sinkCfg)
    cassandraSink.write(logsWithBots)

    spark
      .streams
      .awaitAnyTermination()

  }

}

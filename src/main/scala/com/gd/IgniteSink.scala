package com.gd

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.sql._

class IgniteSink {

  private var spark: SparkSession = _
  private var igniteConfigFile: String = ""
  private var timeoutMs: Option[Long] = None

  def init(timeoutMs: Long = 20000, configFileLocation: String = "build/resources/test/ignite-config.xml"): Unit = {

    spark = SparkSession.builder().getOrCreate()

    this.timeoutMs = Some(timeoutMs)

    igniteConfigFile = configFileLocation
    // "/shared/ignite-config.xml"
  }

  def write(df: DataFrame): Unit = {
    if (df.isStreaming) {
      writeStream(df)
    } else {
      writeBatch(df)
    }

  }

  private def writeBatch(df: DataFrame): Unit = {
    df
      .write.format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, igniteConfigFile)
      .option(OPTION_TABLE, "access_log")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "ip, event_time")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
      .mode(SaveMode.Append) //Overwriting entire table.
      .save()
  }

  private def writeStream(dataFrame: DataFrame): Unit = {
    val query = dataFrame
      .writeStream
      .foreach(streamingWriter(igniteConfigFile, "test_cache", "ignite.cfg", "df-writer-client"))
      .start()

    timeoutMs match {
      case Some(ts) =>
        query.awaitTermination(ts)
      case None =>
        query.awaitTermination()
    }
  }

  private def streamingWriter(igniteConfigFile: String,
                              cacheName: String,
                              configBeanName: String,
                              igniteInstanceName: String
                             ): ForeachWriter[Row] = {
    new ForeachWriter[Row] {
      @transient private var ignite: Ignite = _

      override def open(partitionId: Long, version: Long): Boolean = {

        val loadedCfg: IgniteConfiguration = Ignition.loadSpringBean[IgniteConfiguration](igniteConfigFile, configBeanName)

        loadedCfg.setIgniteInstanceName(igniteInstanceName)

        loadedCfg.setClientMode(true)

        ignite = Ignition.getOrStart(loadedCfg)

        // TODO: use partitionId and version for recovery purposes

        true
      }

      override def process(value: Row): Unit = {
        val cache: IgniteCache[String, String] = ignite.getOrCreateCache[String, String](cacheName)

        cache.put(value.getAs[String]("ip"), value.getAs[String]("url"))
      }

      override def close(errorOrNull: Throwable): Unit = {
        ignite.close()
      }
    }
  }
}

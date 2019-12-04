package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class IgniteSink
  extends Serializable
    with StrictLogging {

  private var spark: SparkSession = _
  private var igniteConfigFile: String = ""
  private var timeoutMs: Option[Long] = None

  private val igniteBatchTable = "access_log"
  private val igniteStreamingCache = "ignite-cassandra"
  private val igniteConfigBean = "ignite.cfg"
  private val igniteInstanceName = "df-writer-client"

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
      .option(OPTION_TABLE, igniteBatchTable)
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "ip, event_time")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
      .mode(SaveMode.Append) //Overwriting entire table.
      .save()
  }

  private def writeStream(dataFrame: DataFrame): Unit = {
    val writer = streamingWriter(
      igniteConfigFile,
      igniteStreamingCache,
      igniteConfigBean,
      igniteInstanceName)

    val query = dataFrame
      .writeStream
      .foreach(writer)
      .start()

    timeoutMs match {
      case Some(ts) =>
        query.awaitTermination(ts)
      case None =>
        query.awaitTermination()
    }
  }

  private def streamingWriter(configFile: String,
                              cacheName: String,
                              configBeanName: String,
                              instanceName: String
                             ): ForeachWriter[Row] = {
    new ForeachWriter[Row] {
      @transient private var ignite: Ignite = _

      override def open(partitionId: Long, version: Long): Boolean = {

        val loadedCfg: IgniteConfiguration = Ignition.loadSpringBean[IgniteConfiguration](configFile, configBeanName)

        loadedCfg.setIgniteInstanceName(instanceName)

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
        Try(errorOrNull) match {
          case Success(nothing) =>
            /* no op */
          case Failure(exception) =>
            logger.error("Received error in Ignite writer", exception)
        }

        logger.info("Shutting down Ignite client.")

        ignite.close()
      }
    }
  }
}

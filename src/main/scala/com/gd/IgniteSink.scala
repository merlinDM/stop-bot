package com.gd

import java.util.Date

import com.gd.model.{Ipfix, IpfixKey}
import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder
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
  private var igniteServerHost = "ignite-00:47500..47509"

  def init(timeoutMs: Option[Long] = Some(20000l),
           configFileLocation: String = "build/resources/test/ignite-config.xml",
           igniteHost: String = "ignite-00:47500..47509"): Unit = {

    spark = SparkSession.builder().getOrCreate()

    this.timeoutMs = timeoutMs

    igniteConfigFile = configFileLocation
    // "/shared/ignite-config.xml"

    igniteServerHost = igniteHost
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
      igniteInstanceName,
      igniteServerHost)

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

    Ignition.stop(igniteInstanceName, false)
  }

  private def streamingWriter(configFile: String,
                              cacheName: String,
                              configBeanName: String,
                              instanceName: String,
                              igniteHost: String
                             ): ForeachWriter[Row] = {
    new ForeachWriter[Row] with StrictLogging {
      @transient private lazy val ignite: Ignite = setupIgnite()

      private def setupIgnite() = {

        logger.debug("Setting up ignite client")

        val loadedCfg: IgniteConfiguration = Ignition.loadSpringBean[IgniteConfiguration](configFile, configBeanName)

        val addresses = new java.util.ArrayList[String](2)
        addresses.add(igniteHost)

        val ipFinder = new TcpDiscoveryMulticastIpFinder
        ipFinder.setAddresses(addresses)

        val spi = new TcpDiscoverySpi()
        spi.setIpFinder(ipFinder)

        loadedCfg.setIgniteInstanceName(instanceName)

        loadedCfg.setClientMode(true)

        val cl = Ignition.getOrStart(loadedCfg)

        cl
      }

      override def open(partitionId: Long, version: Long): Boolean = {

        // TODO: use partitionId and version for recovery purposes

        true
      }

      override def process(value: Row): Unit = {

      val cache: IgniteCache[IpfixKey, Ipfix] = ignite.getOrCreateCache[IpfixKey, Ipfix](cacheName)

      val v = new Ipfix()
      v.setIp(value.getAs[String]("ip"))
      v.setUrl(value.getAs[String]("url"))
      v.setEventTime(value.getAs[Date]("event_time"))
      v.setEventType(value.getAs[String]("event_type"))

      val k = new IpfixKey(v)

      cache.put(k, v)
    }

      override def close(errorOrNull: Throwable): Unit = {
        Try(errorOrNull) match {
          case Success(nothing) =>
            /* no op */
          case Failure(exception) =>
            logger.error("Received error in Ignite writer", exception)
        }

        // we do not stop Ignite client here since this method is called each time after processing new partition.

      }
    }
  }
}

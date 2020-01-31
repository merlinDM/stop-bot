package com.gd

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.gd.model.BotRecord
import com.typesafe.scalalogging.StrictLogging
import javax.cache.expiry.{CreatedExpiryPolicy, Duration}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery, StreamingQueryListener, Trigger}

class IgniteSource(cfg: IgniteSourceConfiguration = IgniteSourceConfiguration())
  extends Serializable
    with StrictLogging {

  private val spark: SparkSession = SparkSession.builder().getOrCreate()
  private val igniteContext: IgniteContext = setupIgniteContext()

  def read(): DataFrame = {
    import spark.implicits._

    val df = igniteContext.fromCache[String, Timestamp](cfg.tableName)
      .toDF("ip", "event_time")

    df
  }

  def write(df: DataFrame): Unit = {
    if (df.isStreaming) {
      writeStream(df)
    } else {
      writeBatch(df)
    }

  }

  private def writeBatch(df: DataFrame): Unit = {
    import spark.implicits._

    lazy val cache = setupCache(igniteContext.ignite())
    val ds = df.as[BotRecord].map(r => (r.ip, r.event_time))

    ds.foreachPartition( it => {
      it.foreach { case (s: String, t: Timestamp) => cache.put(s, t) }
    })

  }

  def writeStream(dataFrame: DataFrame): StreamingQuery = {

    def wrappedWrite(ds: Dataset[Row], idx: Long): Unit = {
      logger.info(s"Writing batch #$idx")
      writeBatch(ds)
    }

    val query = dataFrame
      .writeStream
      .foreachBatch(wrappedWrite _ )
      .trigger(cfg.triggerTime)
      .start()

    query
  }

  def close(): Unit = {
    logger.info(s"Closing Ignite instance ${cfg.instanceName}")
    igniteContext.close(shutdownIgniteOnWorkers = true)
  }

  private def setupIgniteConfiguration(): IgniteConfiguration = {
    if (cfg.silenceIgnite) {
      Logger.getLogger("org.apache.ignite").setLevel(Level.WARN)
    }

    // The file contains discovery settings
    val is = this.getClass.getClassLoader.getResourceAsStream(cfg.configFile)
    val igniteConfig = Ignition.loadSpringBean[IgniteConfiguration](is, cfg.configBean)
    igniteConfig.setIgniteInstanceName(cfg.instanceName)

    igniteConfig.setClientMode(true)

    igniteConfig
  }

  private def setupIgniteContext(): IgniteContext = {
    val igniteConfigFactory: () => IgniteConfiguration = () => setupIgniteConfiguration()

    val listener = new IgniteStreamingQueryListener(igniteConfigFactory, this.close)
    spark.streams.addListener(listener)

    IgniteContext(spark.sparkContext,
      () => setupIgniteConfiguration())
  }

  private def setupCache(ignite: Ignite): IgniteCache[String, Timestamp] = {
    val cacheConfig = new CacheConfiguration[String, Timestamp]()
    cacheConfig.setName(cfg.tableName)

    val expiryPolicyFactory = CreatedExpiryPolicy.factoryOf(cfg.ttl)
    cacheConfig.setExpiryPolicyFactory(expiryPolicyFactory)

    ignite.getOrCreateCache[String, Timestamp](cacheConfig)
  }

}

case class IgniteSourceConfiguration(
  configFile: String = "ignite-client-config.xml",
  timeoutMs: Option[Long] = None,
  configBean: String = "ignite.cfg",
  instanceName: String = "df-writer-client",
  serverHost: String = "ignite-00:47500..47509",
  tableName: String = "",
  cacheTemplate: String = "botListTemplate",
  primaryKey: String = "ip",
  saveMode: SaveMode = SaveMode.Append,
  ttl: Duration = Duration.TEN_MINUTES,
  silenceIgnite: Boolean = true,
  triggerTime: Trigger = Trigger.ProcessingTime(10, TimeUnit.SECONDS)
)

class IgniteStreamingQueryListener(
  igniteConfigFactory: () => IgniteConfiguration,
  closeFu: () => Unit) extends StreamingQueryListener
    with StrictLogging {

  private val MAGIC_STRING = "nflqjmg"

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logger.info(MAGIC_STRING + " query started")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {

    logger.info(MAGIC_STRING + " query progressing")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logger.info(MAGIC_STRING + " trying to close ignite instance")

    closeFu()
  }

}
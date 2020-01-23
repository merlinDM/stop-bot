package com.gd

import java.sql.Timestamp

import com.typesafe.scalalogging.StrictLogging
import javax.cache.expiry.{CreatedExpiryPolicy, Duration}
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.IgniteContext
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql._

class IgniteSource(cfg: IgniteSourceConfiguration = IgniteSourceConfiguration())
  extends Serializable
    with StrictLogging {

  private val spark: SparkSession = SparkSession.builder().getOrCreate()

  // https://apacheignite-sql.readme.io/docs/create-table
  private val dummy: String = "dummyTable"

  def read(): DataFrame = {
    import spark.implicits._

    val igniteContext = new IgniteContext(spark.sparkContext,
      cfg.configFile)

    val df = igniteContext.fromCache[String, Timestamp](cfg.tableName)
      .toDF("ip", "event_time")

    df
//    spark
//      .read
//      .format(FORMAT_IGNITE)
//      .option(OPTION_TABLE, dummy)
//      .option(OPTION_CONFIG_FILE, cfg.configFile)
//      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, cfg.primaryKey)
//      .option(OPTION_DISABLE_SPARK_SQL_OPTIMIZATION, true)
//      .option(OPTION_CREATE_TABLE_PARAMETERS,
//        s"BACKUPS=1, TEMPLATE=${cfg.cacheTemplate}, CACHE_NAME=${cfg.tableName}, KEY_TYPE=VARCHAR, VALUE_TYPE=LONG")
//      .load()
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

    val ds = df.as[BotRecord].map(r => (r.ip, r.event_time))
    ds.foreachPartition( it => {
      val ignite = setupIgnite()
      val cache = setupCache(ignite)
      it.foreach { case (s: String, t: Timestamp) => cache.put(s, t) }
    })

    val ignite = setupIgnite()
    ignite.close()
  }

  private def writeStream(dataFrame: DataFrame): Unit = {

    def f(ds: Dataset[Row], idx: Long): Unit = {
      logger.info(s"Writing batch #$idx")
      writeBatch(ds)
    }

    val query = dataFrame.writeStream.foreachBatch(f _ ).start()

    cfg.timeoutMs match {
      case Some(ts) =>
        query.awaitTermination(ts)
      case None =>
        query.awaitTermination()
    }

  }

  private def setupIgnite(): Ignite = {
    val is = this.getClass.getClassLoader.getResourceAsStream(cfg.configFile)
    val igniteConfig = Ignition.loadSpringBean[IgniteConfiguration](is, cfg.configBean)
    igniteConfig.setIgniteInstanceName(cfg.instanceName)

    igniteConfig.setClientMode(true)

    Ignition.getOrStart(igniteConfig)
  }

  private def setupCache(ignite: Ignite): IgniteCache[String, Timestamp] = {
    val cacheConfig = new CacheConfiguration[String, Timestamp]()
    cacheConfig.setName(cfg.tableName)

    val expiryPolicyFactory = CreatedExpiryPolicy.factoryOf(Duration.TEN_MINUTES)
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
  saveMode: SaveMode = SaveMode.Append
)

case class BotRecord (
  ip: String,
  event_time: Timestamp
)
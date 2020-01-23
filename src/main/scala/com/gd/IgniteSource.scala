package com.gd

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql._

class IgniteSource(cfg: IgniteSourceConfiguration = IgniteSourceConfiguration())
  extends Serializable
    with StrictLogging {

  private val spark: SparkSession = SparkSession.builder().getOrCreate()

  // https://apacheignite-sql.readme.io/docs/create-table
  private val dummy: String = "dummyTable"

  def read(): DataFrame = {
    spark
      .read
      .format(FORMAT_IGNITE)
      .option(OPTION_TABLE, dummy)
      .option(OPTION_CONFIG_FILE, cfg.configFile)
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, cfg.primaryKey)
      .option(OPTION_DISABLE_SPARK_SQL_OPTIMIZATION, true)
      .option(OPTION_CREATE_TABLE_PARAMETERS,
        s"BACKUPS=1, TEMPLATE=${cfg.cacheTemplate}, CACHE_NAME=${cfg.tableName}, KEY_TYPE=VARCHAR, VALUE_TYPE=LONG")
      .load()
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
      .option(OPTION_TABLE, dummy)
      .option(OPTION_CONFIG_FILE, cfg.configFile)
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, cfg.primaryKey)
      .option(OPTION_CREATE_TABLE_PARAMETERS,
        s"BACKUPS=1, TEMPLATE=${cfg.cacheTemplate}, CACHE_NAME=${cfg.tableName}, KEY_TYPE=VARCHAR, VALUE_TYPE=LONG")
      .option(OPTION_STREAMER_ALLOW_OVERWRITE, true)
      .mode(cfg.saveMode)
      .save()
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
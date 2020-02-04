package com.gd

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

class ConsoleSink(cfg: ConsoleSourceConfiguration = ConsoleSourceConfiguration()) {
  private var sparkSession: SparkSession = _

  def write(sdf: DataFrame): Unit = {
    val query: StreamingQuery = sdf.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    cfg.timeoutMs match {
      case Some(ts) =>
        query.awaitTermination(ts)
      case None =>
        query.awaitTermination()
    }


  }

}

case class ConsoleSourceConfiguration(
  timeoutMs: Option[Long] = Some(20000L)
)
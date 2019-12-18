package com.gd

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

class ConsoleSink {
  private var sparkSession: SparkSession = _
  private var timeoutMs: Option[Long] = None

  def init(maybeTimeoutMs: Option[Long] = Some(20000L)): Unit = {
    sparkSession = SparkSession
      .builder()
      .getOrCreate()

    this.timeoutMs = maybeTimeoutMs
  }

  def write(sdf: DataFrame) = {
    val query: StreamingQuery = sdf.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    timeoutMs match {
      case Some(ts) =>
        query.awaitTermination(ts)
      case None =>
        query.awaitTermination()
    }


  }

}

package com.gd

import org.apache.spark.sql.{DataFrame, SparkSession}

class ConsoleSink {
  private var sparkSession: SparkSession = _
  private var timeoutMs: Option[Long] = None

  def init(timeoutMs: Long = 20000): Unit = {
    sparkSession = SparkSession
      .builder()
      .getOrCreate()

    this.timeoutMs = Some(timeoutMs)
  }

  def write(sdf: DataFrame) = {
    val query = sdf.writeStream
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

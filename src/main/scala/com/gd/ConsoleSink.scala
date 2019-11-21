package com.gd

import org.apache.spark.sql.{DataFrame, SparkSession}

class ConsoleSink {
  private var sparkSession: SparkSession = _
  private var timeoutMs: Option[Long] = None

  def init(): Unit = {
    sparkSession = SparkSession
      .builder()
      .getOrCreate()

    timeoutMs = Some(20000)
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

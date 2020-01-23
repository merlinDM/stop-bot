package com.gd.helpers

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class BotRecordsHelper(spark: SparkSession) {

  def read(): DataFrame = {
    spark.createDataFrame(data)
  }

  val data = Array(
    BotRecord(ip = "192.241.194.9",  event_time = new Timestamp(1575987230000L)),
    BotRecord(ip = "192.241.194.11", event_time = new Timestamp(1575987840000L))
  )
}

case class BotRecord (
  ip: String,
  event_time: Timestamp
)
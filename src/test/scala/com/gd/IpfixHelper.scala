package com.gd

import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class IpfixHelper(spark: SparkSession) {

  private var ipfixMS: MemoryStream[Ipfix] = _

  def setupMemoryStream: DataFrame = {
    import spark.implicits.newProductEncoder
    implicit val sql = spark.sqlContext

    val ms = MemoryStream[Ipfix]

    val df = ms.toDF()
      .withColumnRenamed("tpe", "event_type")
      .withColumnRenamed("eventTime", "event_time")
      .withColumn("event_time", col("event_time").cast(TimestampType))

    ipfixMS = ms

    df
  }

  def pushData(): Offset = {
    val head = batches.dequeue
    ipfixMS.addData(head)
  }

  def commitOffsets(offset: Offset): Unit = {
    ipfixMS.commit(offset.asInstanceOf[LongOffset])
  }

  def isEmpty = batches.isEmpty

  private val batches: mutable.Queue[Array[Ipfix]] = mutable.Queue(
    // not bot
    Array(
      Ipfix(url = "https://example-081.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987219)
    ),

    // 2 records per 1 seconds for 20 seconds
    // now it is bot
    Array(
      Ipfix(url = "https://example-076.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987220L),
      Ipfix(url = "https://example-025.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987220L),
      Ipfix(url = "https://example-027.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987221L),
      Ipfix(url = "https://example-087.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987221L),
      Ipfix(url = "https://example-057.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987222L),
      Ipfix(url = "https://example-078.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987222L),
      Ipfix(url = "https://example-080.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987223L),
      Ipfix(url = "https://example-017.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987223L),
      Ipfix(url = "https://example-034.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987224L),
      Ipfix(url = "https://example-074.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987224L),
      Ipfix(url = "https://example-086.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987225L),
      Ipfix(url = "https://example-062.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987225L),
      Ipfix(url = "https://example-049.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987226L),
      Ipfix(url = "https://example-021.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987226L),
      Ipfix(url = "https://example-099.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987227L),
      Ipfix(url = "https://example-068.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987227L),
      Ipfix(url = "https://example-054.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987228L),
      Ipfix(url = "https://example-076.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987228L),
      Ipfix(url = "https://example-025.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987229L),
      Ipfix(url = "https://example-027.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987229L),
      Ipfix(url = "https://example-087.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987230L),
      Ipfix(url = "https://example-057.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987230L),
      Ipfix(url = "https://example-078.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987231L),
      Ipfix(url = "https://example-080.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987231L),
      Ipfix(url = "https://example-017.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987232L),
      Ipfix(url = "https://example-034.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987232L),
      Ipfix(url = "https://example-074.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987233L),
      Ipfix(url = "https://example-076.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987233L),
      Ipfix(url = "https://example-025.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987234L),
      Ipfix(url = "https://example-027.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987234L),
      Ipfix(url = "https://example-087.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987235L),
      Ipfix(url = "https://example-057.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987235L),
      Ipfix(url = "https://example-078.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987236L),
      Ipfix(url = "https://example-080.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987236L),
      Ipfix(url = "https://example-017.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987237L),
      Ipfix(url = "https://example-034.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987237L),
      Ipfix(url = "https://example-074.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987238L),
      Ipfix(url = "https://example-086.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987238L),
      Ipfix(url = "https://example-062.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987239L),
      Ipfix(url = "https://example-049.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987239L)
    ),

    /* =================== 1 m 40 s pause =================== **/

    // it is still bot even though it"s only 1 message
    Array(
      Ipfix(url = "https://example-068.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987320L)
    ),

    /* =================== 9 m 20 s pause =================== **/

    // not bot already
    Array(
      Ipfix(url = "https://example-064.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1575987840L)
    ),

    // Flushes previous record
    Array(
      Ipfix(url = "https://example-064.com/", ip = "192.241.194.9", tpe = "click", eventTime = 1576000000L)
    )
  )
}

case class Ipfix(url: String, ip: String, tpe: String, eventTime: Long)

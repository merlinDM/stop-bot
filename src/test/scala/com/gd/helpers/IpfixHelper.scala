package com.gd.helpers

import java.sql.Timestamp

import com.gd.model.Ipfix
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class IpfixHelper(spark: SparkSession) {

  private var ipfixMS: MemoryStream[Ipfix] = _
  private var currentOffset: Offset = _

  def setupMemoryStream: DataFrame = {
    import spark.implicits.newProductEncoder
    implicit val sql = spark.sqlContext

    val ms = MemoryStream[Ipfix]

    val df = ms.toDF()

    ipfixMS = ms

    df
  }

  def pushData(): Unit = {
    val head = batches.dequeue
    currentOffset = ipfixMS.addData(head)
  }

  def commitOffsets(): Unit = {
    ipfixMS.commit(currentOffset.asInstanceOf[LongOffset])
  }

  def isEmpty: Boolean = batches.isEmpty

  def staticDF: DataFrame = {
    spark.createDataFrame(batches.flatten)
  }

  private val batches: mutable.Queue[Array[Ipfix]] = mutable.Queue(
    // not bot
    Array(
      Ipfix(url = "https://example-081.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987219000L))
    ),

    // 2 records per 1 seconds for 20 seconds
    // now it is bot
    Array(
      Ipfix(url = "https://example-076.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987220000L)),
      Ipfix(url = "https://example-025.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987220000L)),
      Ipfix(url = "https://example-027.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987221000L)),
      Ipfix(url = "https://example-087.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987221000L)),
      Ipfix(url = "https://example-057.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987222000L)),
      Ipfix(url = "https://example-078.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987222000L)),
      Ipfix(url = "https://example-080.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987223000L)),
      Ipfix(url = "https://example-017.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987223000L)),
      Ipfix(url = "https://example-034.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987224000L)),
      Ipfix(url = "https://example-074.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987224000L)),
      Ipfix(url = "https://example-086.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987225000L)),
      Ipfix(url = "https://example-062.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987225000L)),
      Ipfix(url = "https://example-049.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987226000L)),
      Ipfix(url = "https://example-021.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987226000L)),
      Ipfix(url = "https://example-099.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987227000L)),
      Ipfix(url = "https://example-068.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987227000L)),
      Ipfix(url = "https://example-054.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987228000L)),
      Ipfix(url = "https://example-076.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987228000L)),
      Ipfix(url = "https://example-025.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987229000L)),
      Ipfix(url = "https://example-027.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987229000L)),
      Ipfix(url = "https://example-087.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987230000L)),
      Ipfix(url = "https://example-057.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987230000L)),
      Ipfix(url = "https://example-078.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987231000L)),
      Ipfix(url = "https://example-080.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987231000L)),
      Ipfix(url = "https://example-017.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987232000L)),
      Ipfix(url = "https://example-034.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987232000L)),
      Ipfix(url = "https://example-074.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987233000L)),
      Ipfix(url = "https://example-076.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987233000L)),
      Ipfix(url = "https://example-025.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987234000L)),
      Ipfix(url = "https://example-027.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987234000L)),
      Ipfix(url = "https://example-087.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987235000L)),
      Ipfix(url = "https://example-057.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987235000L)),
      Ipfix(url = "https://example-078.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987236000L)),
      Ipfix(url = "https://example-080.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987236000L)),
      Ipfix(url = "https://example-017.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987237000L)),
      Ipfix(url = "https://example-034.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987237000L)),
      Ipfix(url = "https://example-074.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987238000L)),
      Ipfix(url = "https://example-086.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987238000L)),
      Ipfix(url = "https://example-062.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987239000L)),
      Ipfix(url = "https://example-049.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987239000L))
    ),

    /* =================== 1 m 40 s pause =================== **/

    // it is still bot even though it"s only 1 message
    Array(
      Ipfix(url = "https://example-068.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987320000L))
    ),

    /* =================== 9 m 20 s pause =================== **/

    // not bot already
    Array(
      Ipfix(url = "https://example-064.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987840000L))
    ),

    // Flushes previous record
    Array(
      Ipfix(url = "https://example-064.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1576000000000L))
    )
  )
}


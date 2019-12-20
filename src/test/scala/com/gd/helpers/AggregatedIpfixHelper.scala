package com.gd.helpers

import java.sql.Timestamp

import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream, Offset}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class AggregatedIpfixHelper(spark: SparkSession) {

  private var memoryStream: MemoryStream[AggregatedIpfix] = _
  private var currentOffset: Offset = _

  def setupMemoryStream: DataFrame = {
    import spark.implicits.newProductEncoder
    implicit val sql = spark.sqlContext

    val ms = MemoryStream[AggregatedIpfix]

    val df = ms.toDF()

    memoryStream = ms

    df
  }

  def pushData(): Unit = {
    val head = data.dequeue
    currentOffset = memoryStream.addData(head)
  }

  def commitOffsets(): Unit = {
    memoryStream.commit(currentOffset.asInstanceOf[LongOffset])
  }

  def isEmpty = data.isEmpty

  def staticDF: DataFrame = {
    spark.createDataFrame(data)
  }

  private val data = mutable.Queue(
    AggregatedIpfix(ip = "192.241.194.9", num_of_requests = 1 , is_bot = false, window_start = new Timestamp(1575987210000L), window_end = new Timestamp(1575987220000L)),
    AggregatedIpfix(ip = "192.241.194.9", num_of_requests = 20, is_bot = true,  window_start = new Timestamp(1575987220000L), window_end = new Timestamp(1575987230000L)),
    AggregatedIpfix(ip = "192.241.194.9", num_of_requests = 20, is_bot = true,  window_start = new Timestamp(1575987230000L), window_end = new Timestamp(1575987240000L)),
    AggregatedIpfix(ip = "192.241.194.9", num_of_requests = 1 , is_bot = false, window_start = new Timestamp(1575987320000L), window_end = new Timestamp(1575987330000L)),
    AggregatedIpfix(ip = "192.241.194.9", num_of_requests = 1 , is_bot = false, window_start = new Timestamp(1575987840000L), window_end = new Timestamp(1575987850000L))
  )
}

case class AggregatedIpfix(ip: String, num_of_requests: Long, is_bot: Boolean, window_start: Timestamp, window_end: Timestamp)

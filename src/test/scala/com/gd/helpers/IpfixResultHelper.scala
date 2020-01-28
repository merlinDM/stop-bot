package com.gd.helpers

import java.sql.Timestamp

import com.gd.model.IpfixResult
import org.apache.spark.sql.{DataFrame, SparkSession}

class IpfixResultHelper(spark: SparkSession) {

  def staticDF: DataFrame = {
    spark.createDataFrame(data)
  }

  private val data: Array[IpfixResult] = Array(
    // not bot
    IpfixResult(url = "https://example-081.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987219000L), is_bot = false),

    // 2 records per 1 seconds for 20 seconds
    // now it is bot

    IpfixResult(url = "https://example-076.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987220000L), is_bot = true),
    IpfixResult(url = "https://example-025.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987220000L), is_bot = true),
    IpfixResult(url = "https://example-027.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987221000L), is_bot = true),
    IpfixResult(url = "https://example-087.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987221000L), is_bot = true),
    IpfixResult(url = "https://example-057.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987222000L), is_bot = true),
    IpfixResult(url = "https://example-078.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987222000L), is_bot = true),
    IpfixResult(url = "https://example-080.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987223000L), is_bot = true),
    IpfixResult(url = "https://example-017.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987223000L), is_bot = true),
    IpfixResult(url = "https://example-034.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987224000L), is_bot = true),
    IpfixResult(url = "https://example-074.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987224000L), is_bot = true),
    IpfixResult(url = "https://example-086.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987225000L), is_bot = true),
    IpfixResult(url = "https://example-062.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987225000L), is_bot = true),
    IpfixResult(url = "https://example-049.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987226000L), is_bot = true),
    IpfixResult(url = "https://example-021.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987226000L), is_bot = true),
    IpfixResult(url = "https://example-099.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987227000L), is_bot = true),
    IpfixResult(url = "https://example-068.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987227000L), is_bot = true),
    IpfixResult(url = "https://example-054.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987228000L), is_bot = true),
    IpfixResult(url = "https://example-076.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987228000L), is_bot = true),
    IpfixResult(url = "https://example-025.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987229000L), is_bot = true),
    IpfixResult(url = "https://example-027.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987229000L), is_bot = true),
    IpfixResult(url = "https://example-087.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987230000L), is_bot = true),
    IpfixResult(url = "https://example-057.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987230000L), is_bot = true),
    IpfixResult(url = "https://example-078.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987231000L), is_bot = true),
    IpfixResult(url = "https://example-080.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987231000L), is_bot = true),
    IpfixResult(url = "https://example-017.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987232000L), is_bot = true),
    IpfixResult(url = "https://example-034.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987232000L), is_bot = true),
    IpfixResult(url = "https://example-074.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987233000L), is_bot = true),
    IpfixResult(url = "https://example-076.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987233000L), is_bot = true),
    IpfixResult(url = "https://example-025.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987234000L), is_bot = true),
    IpfixResult(url = "https://example-027.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987234000L), is_bot = true),
    IpfixResult(url = "https://example-087.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987235000L), is_bot = true),
    IpfixResult(url = "https://example-057.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987235000L), is_bot = true),
    IpfixResult(url = "https://example-078.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987236000L), is_bot = true),
    IpfixResult(url = "https://example-080.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987236000L), is_bot = true),
    IpfixResult(url = "https://example-017.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987237000L), is_bot = true),
    IpfixResult(url = "https://example-034.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987237000L), is_bot = true),
    IpfixResult(url = "https://example-074.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987238000L), is_bot = true),
    IpfixResult(url = "https://example-086.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987238000L), is_bot = true),
    IpfixResult(url = "https://example-062.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987239000L), is_bot = true),
    IpfixResult(url = "https://example-049.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987239000L), is_bot = true),

    /* =================== 1 m 40 s pause =================== **/

    // it is still bot even though it"s only 1 message
    IpfixResult(url = "https://example-068.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987320000L), is_bot = true),

    /* =================== 9 m 20 s pause =================== **/

    // not bot already
    IpfixResult(url = "https://example-064.com/", ip = "192.241.194.9", event_type = "click", event_time = new Timestamp(1575987840000L), is_bot = false)

    // Flushes previous record
    // Ipfix(url = "https://example-064.com/", ip = "192.241.194.9", tpe = "click", eventTime = new Timestamp(1576000000000L))
  )

}

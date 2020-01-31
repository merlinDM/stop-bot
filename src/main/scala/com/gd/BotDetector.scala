package com.gd

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class BotDetector {

  def init(): Unit = {

  }

  def transform(sdf: DataFrame): DataFrame = {
    throw new NotImplementedError("This method is going to be removed.")
  }

  def aggregate(eventLog: DataFrame): DataFrame = {
    val aggregatedLog = eventLog
      // Value from SLA: user should be registered (put in cache) as bot within 1 minute.
      .withWatermark("event_time", "1 minute")
      // Value from SLA: user is detected as bot if it sends more then 20 requests per 10 seconds.
      // Sliding window (SL) is considered to have the same value as window duration (WD). Take a look at the following
      // example:
      // |  3 | 18 |  3 |  2 |  0 |  0 | ... => packets
      // |  0 |  5 | 10 | 15 | 20 | 25 | ... => t, s
      //
      // If SL would be 5 seconds, user would reach the quota both at [0, 10) interval and at [5,15) interval,
      // being punished twice for the same activity.
      .groupBy(col("ip"), window(col("event_time"), "10 seconds", "10 seconds"))
      .agg(count("*").as("num_of_requests"))
      .withColumn("is_bot", col("num_of_requests") > 20)
      .withColumn("window_start", col("window").getField("start"))
      .withColumn("window_end", col("window").getField("end"))
      .drop("window")

    aggregatedLog
  }

  def join(eventLog: DataFrame, botList: DataFrame): DataFrame = {
    import eventLog.sparkSession.implicits._

    val joinedDF = eventLog.as("raw")
      .join(
        botList.as("agg"),
        expr(
          """
            |raw.ip = agg.ip AND
            |raw.event_time >= agg.event_time AND unix_timestamp(raw.event_time) < unix_timestamp(agg.event_time) + 600
            |""".stripMargin),
        "inner")
      .withColumn("is_bot", $"raw.event_time" >= $"agg.event_time" &&
        unix_timestamp($"raw.event_time") < unix_timestamp($"agg.event_time") + 600
      )

    val fields = (eventLog.schema.fieldNames.map(f => s"raw.$f") ++ Array("is_bot"))
      .map(col)
    joinedDF.select(fields: _*)
  }

}

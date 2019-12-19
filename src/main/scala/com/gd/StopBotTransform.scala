package com.gd

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class StopBotTransform {

  def init(): Unit = {

  }

  def transform(sdf: DataFrame): DataFrame = {
    throw new NotImplementedError("This method is going to be removed.")
  }

  def aggregate(sdf: DataFrame): DataFrame = {
    val aggregatedDF = sdf
      .withWatermark("event_time", "10 minutes")
      .groupBy(col("ip"), window(col("event_time"), "10 seconds", "10 seconds"))
      .agg(count("*").as("num_of_requests"))
      .withColumn("is_bot", col("num_of_requests") >= 20)
      .withColumn("window_start", col("window").getField("start"))
      .withColumn("window_end", col("window").getField("end"))
      .drop("window")
    aggregatedDF
  }

  def join(rawDF: DataFrame, aggregatedDF: DataFrame) = {
    val joinedDF = rawDF.as("raw") //.withColumnRenamed("ip", "rawIp")
      .join(
        aggregatedDF.as("agg"),
        expr(
          """
            |raw.ip = agg.ip AND
            |raw.event_time >= agg.window_start AND raw.event_time < agg.window_end
            |""".stripMargin),
        "inner")

    val fields = (rawDF.schema.fieldNames.map(f => s"raw.$f") ++ Array("agg.is_bot"))
      .map(col _)
    joinedDF.select(fields: _*)
  }
}

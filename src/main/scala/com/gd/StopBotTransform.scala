package com.gd

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class StopBotTransform {

  def transform(sdf: DataFrame): DataFrame = {
    sdf
      .withWatermark("event_time", "2 minutes")
      .groupBy(col("ip"), window(col("event_time"), "10 seconds", "10 seconds"))
      .agg(min("event_time"), max("event_time"), count("*").as("num_of_requests"))
      .withColumn("is_bot", col("num_of_requests") > 20)
  }

}

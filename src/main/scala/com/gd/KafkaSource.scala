package com.gd

import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaSource(@transient val spark: SparkSession) {
  def init(): Unit = {

  }

  def read(): DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-00:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  }
}

package com.gd

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaSource() {

  private var sparkSession: SparkSession = _
  var dataSchema: StructType = _
  val json_string = "json_string"
  val client_id = "client_id"

  def init(): Unit = {
    sparkSession = SparkSession
      .builder()
      .getOrCreate()

    dataSchema = StructType(
      Array(
        StructField("url", StringType, nullable = false),
        StructField("ip", StringType, nullable = false),
        StructField("event_time", TimestampType, nullable = true),
        StructField("type", StringType, nullable = true)
      )
    )
  }

  def read(): DataFrame = {
    val sdf = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-00:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .option("fetchOffset.retryIntervalMs", 10000)
      .option("maxOffsetsPerTrigger", 30)
      .load()
      .selectExpr(s"CAST(key AS STRING) as $client_id", s"CAST(value AS STRING) as $json_string")
    parseJson(sdf)
  }

  val parsed_json_string = "parsed_json_string"

  def parseJson(sdf: DataFrame): DataFrame = {
    sdf
      .withColumn("translated_json_string", translate(col(json_string), "\\", ""))
      .withColumn("extracted_json_string", regexp_extract(col("translated_json_string"), "^\\\"(.+)\\\"", 1))
      .withColumn(parsed_json_string, from_json(col("extracted_json_string"), dataSchema))
      .drop("translated_json_string", "extracted_json_string")
  }
}

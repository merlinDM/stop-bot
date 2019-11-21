package com.gd

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaSource() {

  private var sparkSession: SparkSession = _
  var dataSchema: StructType = _
  val json_string = "json_string"
  val client_id = "client_id"
  val parsed_json_string = "parsed_json_string"

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

    val nested = unescapeIp(parseJson(sdf))

    nested
      .select(
//        col(client_id),
        col(s"$parsed_json_string.url"),
        col(s"$parsed_json_string.ip"),
        col(s"$parsed_json_string.event_time").cast(TimestampType),
        col(s"$parsed_json_string.type")
      )
  }

  def parseJson(sdf: DataFrame): DataFrame = {
    val translated_json_string = "translated_json_string"
    val extracted_json_string = "extracted_json_string"
    sdf
      .withColumn(translated_json_string, translate(col(json_string), "\\", ""))
      .withColumn(extracted_json_string, regexp_extract(col(translated_json_string), "^\\\"(.+)\\\"", 1))
      .withColumn(parsed_json_string, from_json(col(extracted_json_string), dataSchema))
      .drop(translated_json_string, extracted_json_string)
  }

  def unescapeIp(sdf: DataFrame): DataFrame = {
    val translated_client_id = "translated_client_id"
    sdf
      .withColumn(translated_client_id, translate(col(client_id), "\\", ""))
      .withColumn(translated_client_id, translate(col(translated_client_id), "\"", ""))
      .withColumn(client_id, col(translated_client_id))
      .drop(translated_client_id)
  }
}

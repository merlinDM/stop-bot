package com.gd

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaSource() {

  private var sparkSession: SparkSession = _
  var input_json_schema: StructType = _

  val input_column_json = "json_string"
  val output_column_key = "client_id"
  val output_column_parsed_json = "parsed_json_string"

  private val bootstrapServers = "kafka-00:9092"
  private val topic = "test"
  private val startingOffsets = "earliest"
  private val retryInterval = 10000
  private val offsetsPerBatch = 30

  def init(): Unit = {
    sparkSession = SparkSession
      .builder()
      .getOrCreate()

    input_json_schema = StructType(
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
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("fetchOffset.retryIntervalMs", retryInterval)
      .option("maxOffsetsPerTrigger", offsetsPerBatch)
      .load()
      .selectExpr(s"CAST(key AS STRING) as $output_column_key", s"CAST(value AS STRING) as $input_column_json")

    val nested = unescapeIp(parseJson(sdf))

    nested
      .select(
        col(s"$output_column_parsed_json.url"),
        col(s"$output_column_parsed_json.ip"),
        col(s"$output_column_parsed_json.event_time").cast(TimestampType),
        col(s"$output_column_parsed_json.type").as("event_type")
      )
  }

  def parseJson(sdf: DataFrame): DataFrame = {
    assert(sdf.schema.fieldNames.contains(input_column_json), "DataFrame")

    val tmp_column_name = "extracted_json_string"
    sdf
      .withColumn(tmp_column_name, translate(col(input_column_json), "\\", ""))
      .withColumn(tmp_column_name, regexp_extract(col(tmp_column_name), "^\\\"(.+)\\\"", 1))
      .withColumn(output_column_parsed_json, from_json(col(tmp_column_name), input_json_schema))
      .drop(tmp_column_name)
  }

  def unescapeIp(sdf: DataFrame): DataFrame = {


    val tmp_column_name = "translated_client_id"
    sdf
      .withColumn(tmp_column_name, translate(col(output_column_key), "\\", ""))
      .withColumn(tmp_column_name, translate(col(tmp_column_name), "\"", ""))
      .withColumn(output_column_key, col(tmp_column_name))
      .drop(tmp_column_name)
  }
}

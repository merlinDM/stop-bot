package com.gd

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaSource(cfg: KafkaSourceConfiguration = KafkaSourceConfiguration()) {

  private val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  val inputJSONSchema: StructType = StructType(
    Array(
      StructField("url", StringType, nullable = false),
      StructField("ip", StringType, nullable = false),
      StructField("event_time", TimestampType, nullable = true),
      StructField("type", StringType, nullable = true)
    )
  )

  def read(): DataFrame = {
    val sdf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribe", cfg.topic)
      .option("startingOffsets", cfg.startingOffsets)
      .option("fetchOffset.retryIntervalMs", cfg.retryInterval)
      .option("maxOffsetsPerTrigger", cfg.offsetsPerBatch)
      .load()
      .selectExpr(s"CAST(key AS STRING) as ${cfg.outputColumnKey}", s"CAST(value AS STRING) as ${cfg.inputColumnJSON}")

    val nested = unescapeIp(parseJson(sdf))

    nested
      .select(
        col(s"${cfg.outputColumnParsedJSON}.url"),
        col(s"${cfg.outputColumnParsedJSON}.ip"),
        col(s"${cfg.outputColumnParsedJSON}.event_time").cast(TimestampType),
        col(s"${cfg.outputColumnParsedJSON}.type").as("event_type")
      )
  }

  def parseJson(sdf: DataFrame): DataFrame = {
    assert(sdf.schema.fieldNames.contains(cfg.inputColumnJSON), "DataFrame")

    val tmp_column_name = "extracted_json_string"
    sdf
      .withColumn(tmp_column_name, translate(col(cfg.inputColumnJSON), "\\", ""))
      .withColumn(tmp_column_name, regexp_extract(col(tmp_column_name), "^\\\"(.+)\\\"", 1))
      .withColumn(cfg.outputColumnParsedJSON, from_json(col(tmp_column_name), inputJSONSchema))
      .drop(tmp_column_name)
  }

  def unescapeIp(sdf: DataFrame): DataFrame = {


    val tmp_column_name = "translated_client_id"
    sdf
      .withColumn(tmp_column_name, translate(col(cfg.outputColumnKey), "\\", ""))
      .withColumn(tmp_column_name, translate(col(tmp_column_name), "\"", ""))
      .withColumn(cfg.outputColumnKey, col(tmp_column_name))
      .drop(tmp_column_name)
  }
}

case class KafkaSourceConfiguration(
  inputColumnJSON: String = "json_string",
  outputColumnKey: String = "client_id",
  outputColumnParsedJSON: String = "parsed_json_string",
  bootstrapServers: String = "kafka-00:9092",
  topic: String = "test",
  startingOffsets: String = "earliest",
  retryInterval: Int = 10000,
  offsetsPerBatch: Int = 30
)
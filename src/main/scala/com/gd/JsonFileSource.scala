package com.gd

import java.nio.file.FileSystems

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonFileSource extends StrictLogging {

  @transient private val spark: SparkSession = SparkSession.builder().getOrCreate()
  private var _datadir = ""
  private var _readStream: Boolean = _
  private val _dataSchema = StructType(
    Array(
      StructField("url", StringType, nullable = false),
      StructField("ip", StringType, nullable = false),
      StructField("event_time", TimestampType, nullable = true),
      StructField("type", StringType, nullable = true)
    )
  )


  def init(datadir: String = "/data/",
           readStream: Boolean = true): Unit = {
    _datadir = FileSystems.getDefault.getPath(datadir).toAbsolutePath.toString

    _readStream = readStream
  }

  def read(): DataFrame = {
    val df = if (_readStream) {
      spark
        .readStream
        .schema(_dataSchema)
        .json(_datadir)
    } else {
      spark
        .read
        .schema(_dataSchema)
        .json(_datadir)
    }

    df.withColumnRenamed("type", "event_type")

  }

}

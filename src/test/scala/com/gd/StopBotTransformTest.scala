package com.gd

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite
import org.apache.spark.sql.execution.streaming.{MemoryStream, Offset}

class StopBotTransformTest extends FunSuite {

  private val dataFile: String = {
    val pwd = System.getProperty("user.dir")
    val file = s"file:///$pwd/build/resources/test/data"

    file
  }

  private val dataSchema = StructType(
    Array(
      StructField("url", StringType, nullable = false),
      StructField("ip", StringType, nullable = false),
      StructField("event_type", StringType, nullable = true),
      StructField("event_time", TimestampType, nullable = true)
    )
  )

  private val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  private def disableLogs = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  disableLogs

  test("Data Aggregation") {
    val helper = new IpfixHelper(spark)
    val memoryDF = helper.setupMemoryStream

    val transformer = new StopBotTransform()
    transformer.init()
    val aggregatedDF = transformer.aggregate(memoryDF)

    val table = "aggregationQuery"
    val query = aggregatedDF
      .writeStream
      .format("memory")
      .queryName(table)
      .start()

    var currentOffset: Offset = null
    while (!helper.isEmpty) {
      currentOffset = helper.pushData()
    }

    query.processAllAvailable()
    spark.sqlContext
      .table(table)
      .orderBy(col("window_start").asc)
      .show(numRows = 100, truncate = false)

    val rows = spark.sqlContext
      .table(table)
      .count()

    assert(rows == 5, "Aggregation result rows")

    helper.commitOffsets(currentOffset)
  }

  test("Streams Join") {
    val helper = new IpfixHelper(spark)
    val memoryDF = helper.setupMemoryStream

    val transformer = new StopBotTransform()
    transformer.init()
    val aggregatedDF = transformer.aggregate(memoryDF)
    val joinedDF = transformer.join(memoryDF, aggregatedDF)

    val table = "joinedQuery"
    val query = joinedDF
      .writeStream
      .format("memory")
      .queryName(table)
      .start()

    var currentOffset: Offset = null
    while (!helper.isEmpty) {
      currentOffset = helper.pushData()
    }

    query.processAllAvailable()
    spark.sqlContext
      .table(table)
      .orderBy(col("event_time").asc)
      .show(numRows = 100, truncate = false)

    val rows = spark.sqlContext
      .table(table)
      .count()

    assert(rows == 43, "There should be the same amount of rows in joined and original tables.")

    helper.commitOffsets(currentOffset)
    spark.sqlContext.dropTempTable(table)
  }

}

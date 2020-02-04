package com.gd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class BotDetectorTest extends FunSuite {

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
    .config("spark.driver.host", "localhost")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  private def disableLogs = {
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  disableLogs

  test("Data Aggregation") {
    val helper = new helpers.IpfixHelper(spark)
    val memoryDF = helper.setupMemoryStream

    val transformer = new BotDetector()
    val aggregatedDF = transformer.aggregate(memoryDF)

    val table = "aggregationQuery"
    val query = aggregatedDF
      .writeStream
      .format("memory")
      .queryName(table)
      .start()

    while (!helper.isEmpty) {
      helper.pushData()
    }

    query.processAllAvailable()

    val resDF = spark.sqlContext
      .table(table)

    resDF
      .orderBy(col("window_start").asc)
      .show(numRows = 100, truncate = false)

    val expectedDataHelper = new helpers.AggregatedIpfixHelper(spark)
    val expectedDF = expectedDataHelper.staticDF

    assert(resDF.except(expectedDF).isEmpty, "Aggregation result should be as expected.")

    helper.commitOffsets()
  }

  test("Streams Join") {
    val helper = new helpers.IpfixHelper(spark)
    val memoryDF = helper.setupMemoryStream

    val aggregatedDataHelper = new helpers.AggregatedIpfixHelper(spark)
    val aggregatedDF = aggregatedDataHelper.setupMemoryStream

    val transformer = new BotDetector()
    val joinedDF = transformer.join(memoryDF, aggregatedDF)

    val table = "joinedQuery"
    val query = joinedDF
      .writeStream
      .format("memory")
      .queryName(table)
      .start()

    while (!helper.isEmpty || !aggregatedDataHelper.isEmpty) {
      if (!helper.isEmpty)
        helper.pushData()

      if (!aggregatedDataHelper.isEmpty)
        aggregatedDataHelper.pushData()
    }

    query.processAllAvailable()

    val resDF = spark.sqlContext
      .table(table)

    resDF
      .orderBy(col("event_time").asc)
      .show(numRows = 100, truncate = false)

    val expectedDataHelper = new helpers.IpfixResultHelper(spark)
    val expectedDF = expectedDataHelper.staticDF

    // Aggregation doesn't know anything about 10 min timeout.
    assert(resDF.except(expectedDF).where("not unix_timestamp(event_time) = 1575987320").count() == 0)

    helper.commitOffsets()
    aggregatedDataHelper.commitOffsets()
  }

//  test("Use state to impose 10 min timeout") {
//    val aggregatedDataHelper = new helpers.AggregatedIpfixHelper(spark)
//    val expectedDF = aggregatedDataHelper.staticDF
//    val aggregatedDF = aggregatedDataHelper.setupMemoryStream
//
//    val transformer = new StopBotTransform()
//    transformer.init()
//    val timeoutedDF = transformer.applyState(aggregatedDF)
//
//    val table = "timeoutedQuery"
//    val query = timeoutedDF
//      .writeStream
//      .format("memory")
//      .queryName(table)
//      .start()
//
//    while (!aggregatedDataHelper.isEmpty) {
//      aggregatedDataHelper.pushData()
//    }
//
//    query.processAllAvailable()
//
//    val resDF = spark.sqlContext
//      .table(table)
//
//    resDF
//      .orderBy(col("window_start").asc)
//      .show(numRows = 100, truncate = false)
//
//    expectedDF.show(numRows = 100, truncate = false)
//
//    assert(resDF.except(expectedDF).count() == 1)
//    assert(resDF.except(expectedDF).where("not unix_timestamp(window_start) = 1575987320 and is_bot").count() == 0)
//
//  }
}

package com.gd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class StructuredTransformationTest extends FunSuite {

  val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.local.dir", "/tmp")
    .getOrCreate()

  spark.conf.getAll.foreach {
    case (k, v) =>
      println(s"$k => $v")
  }

  test("testTransform") {

    val schema = StructType(
      Array(
        StructField("url", StringType, nullable = false),
        StructField("ip", StringType, nullable = false),
        StructField("event_time", TimestampType, nullable = true),
        StructField("type", StringType, nullable = true)
      )
    )

    val pwd = System.getProperty("user.dir")
    val dataFile = s"file:///$pwd/build/resources/test/"

    val df = spark
      .readStream
      .schema(schema)
      .json(dataFile)

    val query = df
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    val timeoutMs = 1000 * 10

    query.awaitTermination(timeoutMs)

  }

}

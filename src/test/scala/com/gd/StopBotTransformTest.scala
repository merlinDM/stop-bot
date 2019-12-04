package com.gd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

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
      StructField("event_time", TimestampType, nullable = true),
      StructField("type", StringType, nullable = true)
    )
  )

  private val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  spark.conf.getAll.foreach {
    case (k, v) =>
      println(s"$k => $v")
  }

  test("test transformation logic") {
    import org.apache.spark.sql.streaming.Trigger
    import scala.concurrent.duration._

    val timeoutMs = 1000 * 30

    val sdf = spark
      .readStream
      .schema(dataSchema)
      .json(dataFile)

    val transformer = new StopBotTransform()

    val transformedSDF = transformer.transform(sdf)

    val query = transformedSDF
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(timeoutMs.millis))
      .start()

    query.awaitTermination(timeoutMs)

  }

}

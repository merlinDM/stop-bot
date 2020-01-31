package com.gd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, isnull}
import org.scalatest.FunSuite

class KafkaSourceTest extends FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  private val sampleJsonString =
    "\"{\\\"type\\\": \\\"click\\\", \\\"ip\\\": \\\"127.0.0.1\\\", \\\"event_time\\\": 1573640787, \\\"url\\\": \\\"https://example-071.com/\\\"}\\\""
  private val sampleIpString =
    "\"\\\"127.0.0.1\\\"\""

  test("Test json parser method in Kafka source") {
    val cfg = KafkaSourceConfiguration()
    val source = new KafkaSource(cfg)

    val df = spark
      .createDataFrame(Seq(HelperCaseClass(sampleJsonString)))
      .select(col("str").as(cfg.inputColumnJSON))

    val res = source.parseJson(df).cache()

//    res.show(truncate = false)

    val test_result = "test_result"
    val errors = res
      .select(
        isnull(col(cfg.outputColumnParsedJSON)).as(test_result))
      .where(col(test_result))
      .count()

    assert(errors == 0)
  }

  test("Test method that throws escape characters from IP address string") {

    val cfg = KafkaSourceConfiguration()
    val source = new KafkaSource(cfg)

    val df = spark
      .createDataFrame(Seq(HelperCaseClass(sampleIpString)))
      .select(col("str").as(cfg.outputColumnKey))

    val res = source.unescapeIp(df).cache()

//    res.show(truncate = false)

    val test_result = "test_result"
    val errors = res
      .select(
        (col(cfg.outputColumnKey) =!= "127.0.0.1").as(test_result))
      .where(col(test_result))
      .count()

    assert(errors == 0)
  }
}

case class HelperCaseClass(str: String)
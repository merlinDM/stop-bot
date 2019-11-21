package com.gd

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.{col, isnull}

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

  test("testParseJson") {
    val source = new KafkaSource()
    source.init()

    val df = spark
      .createDataFrame(Seq(HelperCaseClass(sampleJsonString)))
      .select(col("str").as(source.json_string))

    val res = source.parseJson(df).cache()

    res.show(truncate = false)

    val test_result = "test_result"
    val errors = res
      .select(
        isnull(col(source.parsed_json_string)).as(test_result))
      .where(col(test_result))
      .count()

    assert(errors == 0)
  }

  test("Throw escape characters from IP address string") {

    val source = new KafkaSource()
    source.init()

    val df = spark
      .createDataFrame(Seq(HelperCaseClass(sampleIpString)))
      .select(col("str").as(source.client_id))

    val res = source.unescapeIp(df).cache()

    res.show(truncate = false)

    val test_result = "test_result"
    val errors = res
      .select(
        (col(source.client_id) =!= "127.0.0.1").as(test_result))
      .where(col(test_result))
      .count()

    assert(errors == 0)
  }
}

case class HelperCaseClass(str: String)
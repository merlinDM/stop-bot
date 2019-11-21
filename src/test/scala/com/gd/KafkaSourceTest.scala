package com.gd

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class KafkaSourceTest extends FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.local.dir", "/tmp/spark")
    .getOrCreate()

  private val sampleString =
    "\"{\\\"type\\\": \\\"click\\\", \\\"ip\\\": \\\"127.0.0.1\\\", \\\"event_time\\\": 1573640787, \\\"url\\\": \\\"https://example-071.com/\\\"}\\\""

  test("testParseJson") {
    val df = spark.createDataFrame(Seq(HelperCaseClass(sampleString)))

    val source = new KafkaSource()
    source.init()
    val res = source.parseJson(df).cache()

    res.show(truncate = false)

    import org.apache.spark.sql.functions.{col, isnull}

    val test_result = "test_result"
    val errors = res
      .select(
        isnull(col(source.parsed_json_string)).as(test_result))
      .where(col(test_result))
      .count()

    assert(errors == 0)
  }

}

case class HelperCaseClass(json_string: String)
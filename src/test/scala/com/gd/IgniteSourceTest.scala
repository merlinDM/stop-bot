package com.gd

import com.gd.helpers.{BotRecordsHelper, IpfixHelper, SetupIgnite}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class IgniteSourceTest extends FunSuite with SetupIgnite {

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[1]")
    conf.setAppName("Test Application")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  }

  test("Write Batch DataFrame") {

    val spark = SparkSession.builder().getOrCreate()

    val botRecordsHelper = new BotRecordsHelper(spark)
    val sample = botRecordsHelper
      .read()

    val sinkCnf = IgniteSourceConfiguration(tableName = "testtable")
    val sink = new IgniteSource(sinkCnf)
    sink.write(sample)

    sample.show()

    val sourceCfg = IgniteSourceConfiguration(tableName = "testtable")
    val source = new IgniteSource(sourceCfg)
    val df = source.read()

    assert(df.exceptAll(sample).isEmpty)
  }

  test("Write Streaming DataFrame") {
    val spark = SparkSession.builder().getOrCreate()

    val sourceDataHelper = new IpfixHelper(spark)
    val sdf0 = sourceDataHelper.setupMemoryStream

    val sdf1 = sdf0.select("ip", "event_time")

    val sinkCnf = IgniteSourceConfiguration(tableName = "streamingtesttable")
    val sink = new IgniteSource(sinkCnf)
    sink.write(sdf1)

    while (!sourceDataHelper.isEmpty) {
      sourceDataHelper.pushData()
    }

    val sourceCfg = IgniteSourceConfiguration(tableName = "streamingtesttable")
    val source = new IgniteSource(sourceCfg)
    val result = source.read()

    val expectedDataHelper = new IpfixHelper(spark)
    val expected = expectedDataHelper.staticDF
      .select("ip", "event_time")

    assert(result.exceptAll(expected).isEmpty)

  }
}

package com.gd

import com.gd.helpers.{BotRecordsHelper, SetupIgnite}
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

}

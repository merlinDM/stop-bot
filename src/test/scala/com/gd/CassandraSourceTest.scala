package com.gd

import com.gd.helpers.IpfixResultHelper
import javax.cache.configuration.Factory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CassandraSourceTest extends FunSuite with BeforeAndAfterAll with SetupSpark {

  val cassandraHost = "localhost"
  override protected def appName: String = "Cassandra Test"

  override protected val spark: SparkSession = setupSpark(cassandraHost = Some(cassandraHost))

  test("Cassandra is up") {

    val helper = new IpfixResultHelper(spark)
    val sampleDF = helper.staticDF

    val sinkCfg = CassandraSourceConfiguration(keyspace = "stopbot", table = "access_log", mode = SaveMode.Append)
    val sink = new CassandraSource(sinkCfg)
    sink.write(sampleDF)

    val sourceCfg = CassandraSourceConfiguration(keyspace = "stopbot", table = "access_log", mode = SaveMode.Append)
    val source = new CassandraSource(sourceCfg)
    val res = source.read()

    assert(res.exceptAll(sampleDF).isEmpty)
  }
}
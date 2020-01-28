package com.gd

import com.gd.helpers.{CassandraClient, IpfixResult, IpfixResultHelper}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CassandraSourceTest extends FunSuite with BeforeAndAfterAll with SetupSpark {

  val cassandraHost = "localhost"
  override protected def appName: String = "Cassandra Test"
  override protected val spark: SparkSession = setupSpark(cassandraHost = Some(cassandraHost))
  val client = new CassandraClient

  override def beforeAll() {
    client.connect(cassandraHost)
    client.truncateTable()
  }

  override def afterAll(): Unit = {
    client.close()
  }

  test("Cassandra is up") {

    val helper = new IpfixResultHelper(spark)
    val sampleDF = helper.staticDF

    sampleDF.show(truncate = false)

    val sinkCfg = CassandraSourceConfiguration(keyspace = "stopbot", table = "access_log", mode = SaveMode.Append)
    val sink = new CassandraSource[IpfixResult](sinkCfg)
    sink.write(sampleDF)

    val sourceCfg = CassandraSourceConfiguration(keyspace = "stopbot", table = "access_log", mode = SaveMode.Append)
    val source = new CassandraSource[IpfixResult](sourceCfg)
    val res = source.read()

    res.show(truncate = false)

    val diff = res.exceptAll(sampleDF)
    diff.show()

    assert(diff.isEmpty)
  }
}
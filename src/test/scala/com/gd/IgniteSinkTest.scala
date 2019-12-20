package com.gd

import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.{Ignite, Ignition}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class IgniteSinkTest extends FunSuite {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Spark Ignite data sources write example")
    .master("local")
    .config("spark.executor.instances", "1")
    .getOrCreate()

  private val configFile: String = {
    val pwd = System.getProperty("user.dir")
    val file = s"file:///$pwd/build/resources/test/ignite-config.xml"

    file
  }


  private val dataFile: String = {
    val pwd = System.getProperty("user.dir")
    val file = s"file:///$pwd/build/resources/test/data/"

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

  private val ignite = setupServerAndData

  test("Write Batch DataFrame") {

    val sdf = spark
      .read
      .schema(dataSchema)
      .json(dataFile)

    val sink = new IgniteSink()
    sink.init()
    sink.write(sdf)

    import scala.collection.JavaConverters._

    ignite.configuration()

    val list = ignite.cacheNames().asScala

    println("Found Ignite Caches:\n" + list.mkString("\t", ";\n\t", ""))

    val df = spark.read
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, configFile)
      .option(OPTION_TABLE, "access_log")
      .load()

    df.printSchema()

    df.show
  }

  test("Write Streaming DataFrame") {
    val sdf = spark
      .readStream
      .schema(dataSchema)
      .json(dataFile)
      .withColumnRenamed("type", "event_type")

    val sink = new IgniteSink()
    sink.init()
    sink.write(sdf)

//    while (true) {
//      Thread.sleep(10 * 60 * 1000)
//    }
  }

  def setupServerAndData: Ignite = {

    System.setProperty("IGNITE_JETTY_PORT", "9093")

    val ignite = Ignition.start()

    Logger.getLogger("org.apache.ignite").setLevel(Level.INFO)

    ignite
  }
}

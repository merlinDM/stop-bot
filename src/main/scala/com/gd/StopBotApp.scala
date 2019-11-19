package com.gd

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.streaming.Trigger

object StopBotApp extends StrictLogging {

  def main(args: Array[String]): Unit = {

    // Set up SparkStreaming
    val spark = SparkSession
      .builder()
      .appName("test")
      // TODO Should master be provided when executing through spark-submit?..
//      .master("local[*]")
      .config("spark.local.dir", "/tmp")
      .getOrCreate()

    // Set up Logging
    logger.info("Starting the application")

    // Load Source properties
    val source = new KafkaSource(spark)
    source.init()
    val sdf = source.read()

    val query = sdf.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination(30 * 1000)


    // Load Sink properties

    // Define Transformations

    // Start Application

    // * Define what to do on shutdown

    // * Define what to do on restart

  }
}

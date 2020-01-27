package com.gd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SetupSpark {

  protected def appName: String
  protected def spark: SparkSession

  protected def setupSpark(name: String = appName, cassandraHost: Option[String] = None): SparkSession = {

    // If app is executed via `gradle run`, `spark.master` might be missing.
    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[*]")
    conf.setAppName(name)

    cassandraHost.foreach(host =>
      conf.set("spark.cassandra.connection.host", host)
    )

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark
  }

}

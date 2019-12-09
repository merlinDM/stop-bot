package com.gd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SetupSpark {

  protected def appName: String
  protected def spark: SparkSession

  protected def setupSpark(name: String = appName): SparkSession = {

    // SparkConf allows us to check whether this app has been submitted via script
    // or executed via `gradle run`
    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[*]")
    conf.setAppName(name)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark
  }

}

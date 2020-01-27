package com.gd

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.datastax.spark.connector._

class CassandraSource(cfg: CassandraSourceConfiguration) {

  def write(df: DataFrame): Unit = ???

  def read(): DataFrame = ???

}

case class CassandraSourceConfiguration(
  keyspace: String,
  table: String,
  mode: SaveMode = SaveMode.Append
)
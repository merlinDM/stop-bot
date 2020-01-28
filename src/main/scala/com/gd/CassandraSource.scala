package com.gd

import com.datastax.driver.core.ProtocolVersion
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe._

class CassandraSource[T <: Product : TypeTag](cfg: CassandraSourceConfiguration) {

  def write(df: Dataset[Row]): Unit = {
    import df.sparkSession.implicits._

    val ds = df.as[T]

    cfg.mode match {
      case SaveMode.Append =>
        ds.rdd.saveToCassandra(cfg.keyspace, cfg.table)
      case SaveMode.ErrorIfExists =>
        val tableDef = TableDef.fromType[T](cfg.keyspace, cfg.table, ProtocolVersion.NEWEST_SUPPORTED)
        ds.rdd.saveAsCassandraTableEx(tableDef)
      case SaveMode.Overwrite =>
        ds.rdd.saveAsCassandraTable(cfg.keyspace, cfg.table)
      case SaveMode.Ignore =>
        ()
    }

  }

  def read(): DataFrame = {
    val spark = SparkSession.getActiveSession.get

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", cfg.keyspace)
      .option("table", cfg.table)
      .load()

    df
  }

}

case class CassandraSourceConfiguration(
  keyspace: String,
  table: String,
  mode: SaveMode = SaveMode.Append
)
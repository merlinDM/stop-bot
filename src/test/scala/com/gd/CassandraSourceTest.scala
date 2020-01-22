package com.gd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class CassandraSourceTest extends FunSuite {

  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "127.0.0.1"
  val keyspace = "test"
  val table = "words"

  private val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.local.dir", "/tmp/spark")
    .config("spark.cassandra.connection.host", cassandraHost)
    .getOrCreate()

  test("Cassandra is up") {

    import com.datastax.spark.connector._

    val sc = spark.sparkContext

    // Write two rows to the table:
    val col = sc.parallelize(Seq(("of", "1200"), ("the", "863")))
    col.saveAsCassandraTable(keyspace, table)

    // Read the table and print its contents:
    val rdd: RDD[CassandraRow] = sc.cassandraTable(keyspace, table)
    rdd.collect().foreach(println)

    sc.stop()

  }
}
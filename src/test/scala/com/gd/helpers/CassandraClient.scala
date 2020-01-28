package com.gd.helpers

import com.datastax.driver.core.{Cluster, Session}


class CassandraClient {
  private var session: Session = _
  private var cluster: Cluster = _

  def connect(node: String): Unit = {
    cluster = Cluster.builder.addContactPoint(node).build()
    session = cluster.connect
  }

  def truncateTable(): Unit = {
    session.execute(
      """ CREATE KEYSPACE IF NOT EXISTS stopbot
        | WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
      """.stripMargin
    )

    session.execute(
      """
        |DROP TABLE IF EXISTS stopbot.access_log;
        |""".stripMargin)

    session.execute(
      """ CREATE TABLE stopbot.access_log (
        |   ip text,
        |   event_type text,
        |   -- epoch with microsecond precision
        |   event_time timestamp,
        |   url text,
        |   is_bot boolean,
        |   -- would've been great if we could provide default value here
        |   -- created_at timeuuid,
        |   PRIMARY KEY (ip, event_time, url)
        |   )
        | WITH CLUSTERING ORDER BY (event_time DESC, url ASC);
        |""".stripMargin
    )
  }

  def close(): Unit = {
    cluster.close()
  }
}
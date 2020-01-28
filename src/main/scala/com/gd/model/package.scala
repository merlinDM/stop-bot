package com.gd

import java.sql.Timestamp

package object model {
  case class AggregatedIpfix(ip: String, num_of_requests: Long, is_bot: Boolean, window_start: Timestamp, window_end: Timestamp)

  case class BotRecord (ip: String, event_time: Timestamp)

  case class Ipfix(url: String, ip: String, event_type: String, event_time: Timestamp)

  case class IpfixResult(ip: String, event_time: Timestamp, url: String, event_type: String, is_bot: Boolean)
}

package com.aws.analytics.topicselector

import com.google.gson.JsonParser
import org.apache.flink.connector.kafka.sink.TopicSelector

class FlinkCDCTopicSelector extends TopicSelector[String] {
  override def apply(t: String): String = {
    // t: kafka source topic、key、value
    val db = JsonParser.parseString(t).getAsJsonObject.get("source").getAsJsonObject.get("db").getAsString
    "flink_cdc_" + db.toLowerCase()
  }
}


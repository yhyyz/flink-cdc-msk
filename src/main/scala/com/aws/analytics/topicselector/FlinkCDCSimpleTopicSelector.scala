package com.aws.analytics.topicselector

import com.aws.analytics.model.CDCModel
import org.apache.flink.connector.kafka.sink.TopicSelector

class FlinkCDCSimpleTopicSelector(topicPrefix:String) extends TopicSelector[CDCModel.CDCKafkaModel] {
  // db,table,partitionKey,value
  override def apply(t: CDCModel.CDCKafkaModel): String = {
    // t: kafka source topic、key、value
    // val db = JsonParser.parseString(t).getAsJsonObject.get("source").getAsJsonObject.get("db").getAsString
    topicPrefix + t.db.toLowerCase()
  }
}
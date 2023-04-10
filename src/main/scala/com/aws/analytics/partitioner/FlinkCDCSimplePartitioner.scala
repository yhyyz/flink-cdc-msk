package com.aws.analytics.partitioner

import com.aws.analytics.model.CDCModel
import com.google.gson.JsonParser
import org.apache.flink.connector.kafka.sink.TopicSelector
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner


class FlinkCDCSimplePartitioner extends FlinkKafkaPartitioner[CDCModel.CDCKafkaModel]{
  // db,table,partitionKey,value
  override def partition(t: CDCModel.CDCKafkaModel, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
//    val kpk = JsonParser.parseString(record).getAsJsonObject.get("source").getAsJsonObject.get("kafka_partition_key").getAsString
    val p = Math.abs(t.partitionKey.hashCode() % partitions.length);
    p
  }
}

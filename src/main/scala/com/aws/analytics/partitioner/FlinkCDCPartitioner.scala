package com.aws.analytics.partitioner

import com.google.gson.JsonParser
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class FlinkCDCPartitioner extends FlinkKafkaPartitioner[String]{
  override def partition(record: String, key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {

    val kpk = JsonParser.parseString(record).getAsJsonObject.get("source").getAsJsonObject.get("kafka_partition_key").getAsString
    val p = Math.abs(kpk.hashCode() % partitions.length);
    p
  }
}



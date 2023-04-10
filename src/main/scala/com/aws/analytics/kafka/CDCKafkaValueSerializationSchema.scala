package com.aws.analytics.kafka

import com.aws.analytics.model.CDCModel
import com.aws.analytics.model.CDCModel.CDCKafkaModel
import org.apache.flink.api.common.serialization.SerializationSchema

class CDCKafkaValueSerializationSchema extends SerializationSchema[CDCKafkaModel] {
  override def serialize(t: CDCModel.CDCKafkaModel) : Array[Byte] = {
    t.value.getBytes()
  }
}

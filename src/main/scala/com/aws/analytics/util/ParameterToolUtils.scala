package com.aws.analytics.util

import com.aws.analytics.model.ParamsModel
import org.apache.flink.api.java.utils.ParameterTool

import java.util
import java.util.Properties

object ParameterToolUtils {


  def fromApplicationProperties(properties: Properties): ParameterTool = {
    val map = new util.HashMap[String, String](properties.size())
    properties.forEach((k, v) => map.put(String.valueOf(k), String.valueOf(v)))
    ParameterTool.fromMap(map)
  }

  def getMySQLCDC2MSKParamsForEMR(parameter: ParameterTool): ParamsModel.MySQLCDC2MSKParamsForEMR = {
    val projectEnv = parameter.get("project_env")
    val disableChaining = parameter.get("disable_chaining")
    val deliveryGuarantee = parameter.get("delivery_guarantee")
    val host = parameter.get("host")
    val username = parameter.get("username")
    val password = parameter.get("password")
    val position = parameter.get("position")
    val dbList = parameter.get("db_list")
    val tbList = parameter.get("tb_list")
    val serverId = parameter.get("server_id")
    val serverTimeZone = parameter.get("server_time_zone")
    val kafkaBroker = parameter.get("kafka_broker")
    val topic = parameter.get("topic")
    val topicPrefix = parameter.get("topic_prefix")
    val tablePK = parameter.get("table_pk")
    val checkpointInterval = parameter.get("checkpoint_interval")
    val checkpointDir = parameter.get("checkpoint_dir")
    val parallel = parameter.get("parallel")
    val kafkaProperties = parameter.get("kafka_properties")
    val chunkSize = parameter.get("chunk_size")

    val params = ParamsModel.MySQLCDC2MSKParamsForEMR.apply(projectEnv, disableChaining, deliveryGuarantee, host, username, password, position, dbList, tbList, serverId, serverTimeZone, kafkaBroker, topic, topicPrefix,
      tablePK,checkpointInterval,checkpointDir,parallel,kafkaProperties,chunkSize)
    params
  }
  def getMySQLCDC2MSKParams(parameter: ParameterTool): ParamsModel.MySQLCDC2MSKParams = {
    val projectEnv = parameter.get("project_env")
    val disableChaining = parameter.get("disable_chaining")
    val deliveryGuarantee= parameter.get("delivery_guarantee")
    val host = parameter.get("host")
    val username = parameter.get("username")
    val password = parameter.get("password")
    val position = parameter.get("position")
    val dbList = parameter.get("db_list")
    val tbList = parameter.get("tb_list")
    val serverId = parameter.get("server_id")
    val serverTimeZone = parameter.get("server_time_zone")
    val kafkaBroker = parameter.get("kafka_broker")
    val topic = parameter.get("topic")
    val topicPrefix = parameter.get("topic_prefix")
    val tablePK = parameter.get("table_pk")
    val kafkaProperties = parameter.get("kafka_properties")
    val chunkSize = parameter.get("chunk_size")
    val params = ParamsModel.MySQLCDC2MSKParams.apply(projectEnv,disableChaining,deliveryGuarantee,host,username,password,position,dbList,tbList,serverId,serverTimeZone,kafkaBroker,
      topic,topicPrefix,tablePK,kafkaProperties,chunkSize)
    params
  }

  def getMongoCDC2MSKParams(parameter: ParameterTool): ParamsModel.MongoCDC2MSKParams = {
    val projectEnv = parameter.get("project_env")
    val disableChaining = parameter.get("disable_chaining")
    val deliveryGuarantee = parameter.get("delivery_guarantee")
    val host = parameter.get("host")
    val username = parameter.get("username")
    val password = parameter.get("password")
    val copyExisting = parameter.get("copy_existing")
    val dbList = parameter.get("db_list")
    val collectionList = parameter.get("collection_list")
    val kafkaBroker = parameter.get("kafka_broker")
    val topic = parameter.get("topic")
    val topicPrefix = parameter.get("topic_prefix")
    val kafkaProperties = parameter.get("kafka_properties")
    val params = ParamsModel.MongoCDC2MSKParams.apply(projectEnv, disableChaining, deliveryGuarantee, host, username, password, copyExisting, dbList, collectionList, kafkaBroker, topic, topicPrefix,kafkaProperties)
    params
  }
}

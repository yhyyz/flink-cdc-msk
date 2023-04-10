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

  def getCDC2MSKParams(parameter: ParameterTool): ParamsModel.CDC2MSKParams = {
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
    val params = ParamsModel.CDC2MSKParams.apply(projectEnv,disableChaining,deliveryGuarantee,host,username,password,position,dbList,tbList,serverId,serverTimeZone,kafkaBroker,topic,topicPrefix,tablePK)
    params
  }
}

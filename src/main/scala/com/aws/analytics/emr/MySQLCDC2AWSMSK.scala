package com.aws.analytics.emr

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.MySQLCDC2AWSMSK.gson
import com.aws.analytics.kafka.{CDCKafkaKeySerializationSchema, CDCKafkaValueSerializationSchema}
import com.aws.analytics.model.{CDCModel, ParamsModel}
import com.aws.analytics.partitioner.FlinkCDCSimplePartitioner
import com.aws.analytics.topicselector.FlinkCDCSimpleTopicSelector
import com.aws.analytics.util.ParameterToolUtils
import com.google.gson.{GsonBuilder, JsonElement, JsonParser}
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.logging.log4j.LogManager

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object MySQLCDC2AWSMSK {
  private val log = LogManager.getLogger(MySQLCDC2AWSMSK.getClass)
  private val gson = new GsonBuilder().serializeNulls().create

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val config = new Configuration()
//    config.setString("execution.savepoint.path", "file:///Users/chaopan/Desktop/checkpoint/770351c9f92a0700834255121600dfcd/chk-12")
    val env = StreamExecutionEnvironment.getExecutionEnvironment(config)
    // 注意在Kinesis Analysis 运行时中该参数不生效，需要在CLI中设置相关参数，同时KDA 默认会使用RocksDB存储状态，不用设置
    val  parameter = ParameterTool.fromArgs(args)
    val params = ParameterToolUtils.getMySQLCDC2MSKParamsForEMR(parameter)
    log.info("cdc2kafka: " + params.toString)
    if (params.disableChaining=="true"){
        env.disableOperatorChaining()
    }
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//      3,
//      Time.seconds(10)
//    ))
    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new EmbeddedRocksDBStateBackend())
    env.getCheckpointConfig.setCheckpointStorage(params.checkpointDir)
    env.setParallelism(params.parallel.toInt)
    //[{"db":"test_db","table":"product","primary_key":"pid"},{"db":"test_db","table":"product_01","primary_key":"pid"}]
    val tablePKList = JsonParser.parseString(params.tablePK.replace("\\","")).getAsJsonArray.asList().toArray()
    val tablePKMap =mutable.Map[String,mutable.Map[String,String]]()
    for (item <- tablePKList){
      val jsonEle =item.asInstanceOf[JsonElement]
      val db = jsonEle.getAsJsonObject.get("db").getAsString
      val table = jsonEle.getAsJsonObject.get("table").getAsString
      val primary_key = jsonEle.getAsJsonObject.get("primary_key").getAsString
      val paramsMap = mutable.Map[String, String]()
      paramsMap.put("primary_key", primary_key)
      if (jsonEle.getAsJsonObject.has("column_max_length")) {
        val column_max_length = jsonEle.getAsJsonObject.get("column_max_length").getAsString
        paramsMap.put("column_max_length", column_max_length)
      }
      tablePKMap.put(db + "=" + table, paramsMap)
    }
    val tablePKMapKeyList = tablePKMap.keys.seq.toList.sortBy(- _.length)
//    val chkConfig = env.getCheckpointConfig
//    chkConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    chkConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/chaopan/Desktop/checkpoint/"))
      // 20971520
    //{"before":null,"after":{"pid":1,"pname":"prodcut-001","pprice":"125.12","create_time":"2023-02-14T03:16:38Z","modify_time":"2023-02-14T03:16:38Z"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1678634463000,"snapshot":"false","db":"test_db","sequence":null,"table":"product_01","server_id":57330068,"gtid":null,"file":"mysql-bin-changelog.007670","pos":804,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1678634463898,"transaction":null}
    val mySqlSource = createCDCSource(params)
    val source:DataStreamSource[String] = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc source")

    val mapSource = source.rebalance.map(line => {
      val jsonElement = JsonParser.parseString(line)
      val jsonElementSource = JsonParser.parseString(line).getAsJsonObject.get("source")
      val db = jsonElementSource.getAsJsonObject.get("db").getAsString
      val table = jsonElementSource.getAsJsonObject.get("table").getAsString
      val op = jsonElement.getAsJsonObject.get("op").getAsString
      // get primary key columns from config
      var pk = ""
      var columnMaxLength = ""
      breakable {
        for (k <- tablePKMapKeyList) {
          val reg = k.r
          val p = reg.findFirstIn(db + "=" + table)
          if (p.nonEmpty) {
            pk = tablePKMap.getOrElse(k, mutable.Map[String, String]()).getOrElse("primary_key", "")
            columnMaxLength = tablePKMap.getOrElse(k, mutable.Map[String, String]()).getOrElse("column_max_length", "")
            break
          }
        }
      }
      if (pk != "") {
        val pkValue = ArrayBuffer[String]()
        for (i <- pk.split("\\.")) {
          if (op == "d") {
            pkValue.append(jsonElement.getAsJsonObject.get("before").getAsJsonObject.get(i).getAsString)
          } else {
            pkValue.append(jsonElement.getAsJsonObject.get("after").getAsJsonObject.get(i).getAsString)
          }
        }
        val partitionKey = db + "." + table + "." + pkValue.mkString(".")
        if (columnMaxLength != "") {
          for (item <- columnMaxLength.split("\\|")) {
            val col = item.split("=")(0)
            val maxLength = item.split("=")(1).toInt
            var modifyKey = ""
            if (op == "d") {
              modifyKey = "before"
            } else {
              modifyKey = "after"
            }
            val modifyJsonObj = jsonElement.getAsJsonObject.get(modifyKey).getAsJsonObject
            if (modifyJsonObj.get(col) != null && !modifyJsonObj.get(col).isJsonNull) {
              val colValue = modifyJsonObj.get(col).getAsString
              if (colValue != "" && colValue != null && colValue.length >= maxLength) {
                modifyJsonObj.addProperty(col, colValue.substring(0, maxLength))
              }
            }
          }
        }
        CDCModel.CDCKafkaModel(db, table, partitionKey,  gson.toJson(jsonElement))
      } else {
        val partitionKey = db + "." + table + ".no_pk"
        if (columnMaxLength != "") {
          for (item <- columnMaxLength.split("\\|")) {
            val col = item.split("=")(0)
            val maxLength = item.split("=")(1).toInt
            var modifyKey = ""
            if (op == "d") {
              modifyKey = "before"
            } else {
              modifyKey = "after"
            }
            val modifyJsonObj = jsonElement.getAsJsonObject.get(modifyKey).getAsJsonObject
            if (modifyJsonObj.get(col) != null && !modifyJsonObj.get(col).isJsonNull) {
              val colValue = modifyJsonObj.get(col).getAsString
              if (colValue != "" && colValue != null && colValue.length >= maxLength) {
                modifyJsonObj.addProperty(col, colValue.substring(0, maxLength))
              }
            }
          }
        }
        CDCModel.CDCKafkaModel(db, table, partitionKey, gson.toJson(jsonElement))
      }
    }).filter(line=>line!=null)
//    mapSource.print().setParallelism(1)
    mapSource.sinkTo(createKafkaSink(params))
    env.execute("MySQL Snapshot + Binlog + MSK")
  }

  def createKafkaSink(params:ParamsModel.MySQLCDC2MSKParamsForEMR): KafkaSink[CDCModel.CDCKafkaModel]={
    val properties = new Properties()
    properties.setProperty("acks", "-1")
    properties.setProperty("transaction.timeout.ms", "900000")
//    properties.setProperty("max.request.size", "4788524")

    if ( params.kafkaProperties!="" &&  params.kafkaProperties!=null){
      val proList = params.kafkaProperties.split(",")
      for(kv <- proList){
        val key = kv.split("=")(0)
        val value = kv.split("=")(1)
        properties.setProperty(key, value)
      }
    }

    var dg = DeliveryGuarantee.EXACTLY_ONCE
    if (params.deliveryGuarantee=="at_least_once"){
      dg = DeliveryGuarantee.AT_LEAST_ONCE
    }
    if (params.topicPrefix=="" || params.topicPrefix==null){
      lazy val kafakSink = KafkaSink.builder()
        .setDeliverGuarantee(dg)
        .setBootstrapServers(params.kafkaBroker)
        .setKafkaProducerConfig(properties)
        .setRecordSerializer(
          KafkaRecordSerializationSchema.builder()
            .setPartitioner(new FlinkCDCSimplePartitioner())
            .setKeySerializationSchema(new CDCKafkaKeySerializationSchema())
            .setValueSerializationSchema(new CDCKafkaValueSerializationSchema())
            .setTopic(params.topic)
            .build())
        .build()
       kafakSink
    }else{
      lazy val kafakSink = KafkaSink.builder()
        .setDeliverGuarantee(dg)
        .setBootstrapServers(params.kafkaBroker)
        .setKafkaProducerConfig(properties)
        .setRecordSerializer(
          KafkaRecordSerializationSchema.builder()
            .setTopicSelector(new FlinkCDCSimpleTopicSelector(params.topicPrefix))
            .setPartitioner(new FlinkCDCSimplePartitioner())
            .setKeySerializationSchema(new CDCKafkaKeySerializationSchema())
            .setValueSerializationSchema(new CDCKafkaValueSerializationSchema())
            .build())
        .build()

       kafakSink
    }

  }

  def createCDCSource(params:ParamsModel.MySQLCDC2MSKParamsForEMR): MySqlSource[String]={
    var startPos=StartupOptions.initial()
    if (params.position!=null && ""!=params.position){
      if (params.position == "latest") {
        startPos = StartupOptions.latest()
      } else if (params.position.contains("mysql")) {
        val tmp = params.position.split(":")
        if (tmp.length > 1) {
          val file = tmp(0)
          val pos = tmp(1)
          startPos = StartupOptions.specificOffset(file, pos.toLong)
        } else {
          val file = tmp(0)
          startPos = StartupOptions.specificOffset(file, 4L)
        }
      } else if (params.position.contains("gtid:")) {
        startPos = StartupOptions.specificOffset(params.position.split("gtid:")(1))
      } else if (params.position.contains("timestamp:")){
        startPos = StartupOptions.timestamp(params.position.split("timestamp:")(1).toLong)
      }
    }

    val prop = new Properties()
    prop.setProperty("decimal.handling.mode","string")
    prop.setProperty("bigint.unsigned.handling.mode", "long")

    prop.put("converters", "CDCDateConvert")
    prop.put("CDCDateConvert.type", "com.aws.analytics.tools.DebeziumConverter")
    prop.put("CDCDateConvert.database.type", "mysql")
    var splitSize = 8096
    if (params.chunkSize != "" && params.chunkSize != null) {
      splitSize = params.chunkSize.toInt
    }

    MySqlSource.builder[String]
      .hostname(params.host.split(":")(0))
      .port(params.host.split(":")(1).toInt)
      .username(params.username)
      .password(params.password)
      .databaseList(params.dbList)
      .tableList(params.tbList)
      .startupOptions(startPos)
      .serverId(params.serverId)
      .serverTimeZone(params.serverTimeZone)
      .splitSize(splitSize)
      .debeziumProperties(prop)
      .includeSchemaChanges(false)
      .deserializer(new JsonDebeziumDeserializationSchema(false)).build
  }


}

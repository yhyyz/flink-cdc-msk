package com.aws.analytics

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.kafka.{CDCKafkaKeySerializationSchema, CDCKafkaValueSerializationSchema}
import com.aws.analytics.model.CDCModel.CDCKafkaModel
import com.aws.analytics.model.{CDCModel, ParamsModel}
import com.aws.analytics.partitioner.{FlinkCDCPartitioner, FlinkCDCSimplePartitioner}
import com.aws.analytics.topicselector.{FlinkCDCSimpleTopicSelector, FlinkCDCTopicSelector}
import com.aws.analytics.util.ParameterToolUtils
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.logging.log4j.LogManager

import java.util.Properties
import com.google.gson.{GsonBuilder, JsonElement, JsonParser}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
import org.apache.kafka.clients.producer.ProducerRecord

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
    env.enableCheckpointing(5000)

    var parameter: ParameterTool = null
    if (env.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // 使用KDA Runtime获取参数数
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.getMySQLCDC2MSKParams(parameter)
    log.info("cdc2kafka: " + params.toString)
    if (params.disableChaining=="true"){
        env.disableOperatorChaining()
    }
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3,
      Time.seconds(10)
    ))

    //[{"db":"test_db","table":"product","primary_key":"pid"},{"db":"test_db","table":"product_01","primary_key":"pid"}]
    val tablePKList = JsonParser.parseString(params.tablePK).getAsJsonArray.asList().toArray()
    val tablePKMap =mutable.Map[String,String]()
    for (item <- tablePKList){
      val jsonEle =item.asInstanceOf[JsonElement]
      val db = jsonEle.getAsJsonObject.get("db").getAsString
      val table = jsonEle.getAsJsonObject.get("table").getAsString
      val primary_key = jsonEle.getAsJsonObject.get("primary_key").getAsString
      tablePKMap.put(db+"="+table,primary_key)
    }
    val tablePKMapKeyList = tablePKMap.keys.seq.toList.sortBy(- _.length)
//    val chkConfig = env.getCheckpointConfig
//    chkConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    chkConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/chaopan/Desktop/checkpoint/"))

    //{"before":null,"after":{"pid":1,"pname":"prodcut-001","pprice":"125.12","create_time":"2023-02-14T03:16:38Z","modify_time":"2023-02-14T03:16:38Z"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1678634463000,"snapshot":"false","db":"test_db","sequence":null,"table":"product_01","server_id":57330068,"gtid":null,"file":"mysql-bin-changelog.007670","pos":804,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1678634463898,"transaction":null}
    val mySqlSource = createCDCSource(params)
    val source:DataStreamSource[String] = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc source")

    val mapSource = source.rebalance.map(line=>{
      val jsonElement = JsonParser.parseString(line)
      val jsonElementSource = JsonParser.parseString(line).getAsJsonObject.get("source")
      val db =  jsonElementSource.getAsJsonObject.get("db").getAsString
      val table =  jsonElementSource.getAsJsonObject.get("table").getAsString
      val op = jsonElement.getAsJsonObject.get("op").getAsString
      // get primary key columns from config
      var pk = ""
      breakable{ for (k <- tablePKMapKeyList) {
        val reg = k.r
        val p = reg.findFirstIn(db + "=" + table)
        if (p.nonEmpty) {
          pk = tablePKMap.getOrElse(k, "")
          break
        }
      }}
      if (pk !=""){
        val pkValue = ArrayBuffer[String]()
        for (i <-pk.split("\\.")){
          if (op=="d"){
            pkValue.append(jsonElement.getAsJsonObject.get("before").getAsJsonObject.get(i).getAsString)
          }else{
            pkValue.append(jsonElement.getAsJsonObject.get("after").getAsJsonObject.get(i).getAsString)
          }
        }
        val partitionKey = db+"."+table+"."+pkValue.mkString(".")
        CDCModel.CDCKafkaModel(db,table,partitionKey,gson.toJson(jsonElement))
      }else{
        val partitionKey = db+"."+table+".no_pk"
        CDCModel.CDCKafkaModel(db,table,partitionKey,gson.toJson(jsonElement))
      }
    })
//   mapSource.print().setParallelism(1)

    mapSource.sinkTo(createKafkaSink(params))
    env.execute("MySQL Snapshot + Binlog + MSK")
  }

  def createKafkaSink(params:ParamsModel.MySQLCDC2MSKParams): KafkaSink[CDCModel.CDCKafkaModel]={
    val properties = new Properties()
    properties.setProperty("acks", "-1")
    properties.setProperty("transaction.timeout.ms","900000")
    if (params.kafkaProperties == "" || params.kafkaProperties == null) {
      val proList = params.kafkaProperties.split(",")
      for (kv <- proList) {
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

  def createCDCSource(params:ParamsModel.MySQLCDC2MSKParams): MySqlSource[String]={
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
      .debeziumProperties(prop)
      .includeSchemaChanges(false)
      .deserializer(new JsonDebeziumDeserializationSchema(false)).build
  }



}

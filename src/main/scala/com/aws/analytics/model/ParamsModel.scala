package com.aws.analytics.model

object ParamsModel {

  case class CDC2MSKParams(projectEnv:String,disableChaining:String,  deliveryGuarantee:String,host:String,username:String,
                           password:String,position:String,dbList:String,tbList:String,
                           serverId:String,serverTimeZone:String,kafkaBroker:String,topic:String,topicPrefix:String,tablePK:String)

}

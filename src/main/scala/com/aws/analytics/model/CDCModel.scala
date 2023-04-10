package com.aws.analytics.model

object CDCModel {
  case class CDCKafkaModel(db:String,table:String,partitionKey:String,value:String)

}

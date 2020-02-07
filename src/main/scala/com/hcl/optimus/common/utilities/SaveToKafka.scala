package com.hcl.optimus.common.utilities

import com.hcl.optimus.common.log.logger.Logging
import org.json.JSONObject
import org.apache.kafka.clients.producer._
import java.util.Properties


object SaveToKafka extends Logging {

  def saveDataToKafka(row_key_prefix : String ): (Boolean) = {
    
		
		var passFalg = true
		val jsonObject = new JSONObject( )
        jsonObject.put( "record", "A record" )
        jsonObject.put( "level", "DPERROR" )
        jsonObject.put( "message", "Rule failed while validating data" )
	  
	  logCustomLevel( "DPERROR", row_key_prefix, jsonObject.toString() )
		
		passFalg
  }
  
  def getKafkaProducer(bootstarp: String): (org.apache.kafka.clients.producer.KafkaProducer[String, String]) = {
  
			val  props = new Properties()
			props.put("bootstrap.servers", bootstarp )
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

			val producer = new KafkaProducer[String, String](props)
			producer
  }
  
  def InsertMessageToKafka (key : String, value: String, producer: org.apache.kafka.clients.producer.KafkaProducer[String, String], TOPIC: String ): (Boolean) = {
			
			val record = new ProducerRecord(TOPIC, key , value)
			producer.send(record)

			//producer.close()
			
			true
	}
	def closeProducer (producer: org.apache.kafka.clients.producer.KafkaProducer[String, String]): (Boolean) = {

		producer.close()

		true
	}
}
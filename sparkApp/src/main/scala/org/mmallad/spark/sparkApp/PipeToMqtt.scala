package org.mmallad.spark.sparkApp

import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttClient}

/**
 * Created by Dipak Malla 
    Date: 9/7/14
 */
class PipeToMqtt extends java.io.Serializable{
  def Send(msg:String) = {
    new Runnable {
      override def run(mqttClient:MqttClient): Unit = {
        val mqttClientPersistence = new MemoryPersistence()
        val mqttClient = new MqttClient("tcp://%s:%d".format("localhost",1883),
          MqttClient.generateClientId(),mqttClientPersistence)
        mqttClient.connect()
        println(msg)
        if(mqttClient.isConnected){
          mqttClient.publish("stream/process",msg.getBytes,1, false)
          mqttClient.disconnect()
          mqttClient.close()
        }
      }
    }.run()
  }
}

package org.mmallad.spark.ingest

import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttClient}

import scala.util.Random

/**
 * Created by Dipak Malla 
    Date: 9/6/14
 */
class MqttIngest {
  private var mqttClient : MqttClient = _
  private var mqttClientPersistence : MqttClientPersistence = _
  def Connect() = {
    mqttClientPersistence = new MemoryPersistence()
    mqttClient = new MqttClient("tcp://%s:%d".format("localhost",1883),
      MqttClient.generateClientId(),mqttClientPersistence)
    mqttClient.connect()
  }
  def Send(msg:String) = {
    mqttClient.publish("mqtt/stream",msg.getBytes,1, false)
  }
  def Close() = {
    mqttClient.disconnect()
    mqttClient.close()
  }
}
object MqttIngest{
  def random() : String = {
    val s = List("a","b","c", "d", "e", "f", "g", "h", "i", "j", "k", "l","m",
      "n","o","p","q","r","s","t","u","v","w","x","y","z")
    Random.shuffle(s).mkString("").substring(0,8)
  }
  def main(args: Array[String]) : Unit = {
    val mqtt = new MqttIngest
    mqtt.Connect()
    println("Mqtt Started")
    while(true){
      val m =  random()
      println(m)
      mqtt.Send(m)
      Thread.sleep(1000)
    }
    mqtt.Close()
  }
}

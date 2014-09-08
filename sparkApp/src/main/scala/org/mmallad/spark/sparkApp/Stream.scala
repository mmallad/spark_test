package org.mmallad.spark.sparkApp

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.mqtt._


/**
 * Created by Dipak Malla 
    Date: 9/6/14
 */
object Stream{
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Spark Streaming Example")
    val mqttStream = new StreamingContext(conf, Seconds(2))
    val pipe = new PipeToMqtt()
    mqttStream.checkpoint(".")
    val updateState = (values: Seq[Int], state: Option[Int]) => {
      val cCount = values.foldLeft(0)(_ + _)
      val pCount = state.getOrElse(0)
      Some(cCount + pCount)
    }
    val words = MQTTUtils.createStream(mqttStream, "tcp://localhost:1883", "mqtt/stream",
      StorageLevel.MEMORY_ONLY_SER_2)
    val letters = words.flatMap(l => l.toCharArray)
    val letterCount = letters.map(m => (m, 1)).reduceByKey(_ + _)
    val finalResult = letterCount.updateStateByKey[Int](updateState)
    //finalResult.print()
    finalResult.foreachRDD{
      rdd => rdd.foreach{
        f => pipe.Send(String.valueOf(f._1+"=>"+f._2))
      }
    }
    mqttStream.start()
    mqttStream.awaitTermination()
  }
}

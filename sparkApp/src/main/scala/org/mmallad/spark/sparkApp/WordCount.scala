package org.mmallad.spark.sparkApp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by Dipak Malla 
    Date: 9/5/14
 */
object WordCount {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Word Count Example")
    val sparkContext = new SparkContext(conf)
    val hdfsFile = sparkContext.textFile("hdfs://localhost:54321/spark/bigdata.txt")
    val words = hdfsFile.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_+_)
    val wc = wordCount.collect()
    wc.foreach(f => println(f._1 +" "+f._2))
    wordCount.saveAsTextFile("hdfs://localhost:54321/test/sparkResult2.txt")
    sparkContext.stop()
  }
}
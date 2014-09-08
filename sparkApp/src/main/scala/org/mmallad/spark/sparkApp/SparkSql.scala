package org.mmallad.spark.sparkApp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._

/**
 * Created by Dipak Malla 
    Date: 9/6/14
 */
case class KeyValue(name:String, value:Int)
object SparkSql {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Spark Sql Example")
    val sparkContext = new SparkContext(conf)
    val sparkSql = new SQLContext(sparkContext)
    import sparkSql._
    val hdfsFile = sparkContext.textFile("hdfs://localhost:54321/test/test.txt")
    val words = hdfsFile.flatMap(_.split(" ")).map(k => (k,1)).reduceByKey(_+_).map(d => KeyValue(d._1,d._2))
    words.registerAsTable("wordcount")
    val keys = sparkSql.sql("SELECT * from wordcount where value > 2")
    keys.map(t => t(0)+"=>"+t(1)).collect().foreach(println)
    sparkContext.stop()
  }
}

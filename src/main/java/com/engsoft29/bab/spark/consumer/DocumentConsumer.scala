package com.engsoft29.bab.spark.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

object DocumentConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DocumentConsumer").setMaster("spark://casa-Lenovo:7077")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "bab-document-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("documents")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.map(record => (record.key, record.value)).print()
  }
}
package com.engsoft29.bab.spark.consumer

import java.security.MessageDigest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.rdd.EsSpark

import spray.json._
import DefaultJsonProtocol._

object DocumentConsumer {

  private def buildResult(jsonStr: String): Map[String, Any] = {
    case class Model(url: String, document: String, urls: Array[String])
    implicit val modelFormat = jsonFormat3(Model)
    val json = jsonStr.parseJson.convertTo[Model]
    val summary = json.document.subSequence(0, Math.min(json.document.length(), 150))

    val pagerank = 0d

    Map("id" -> hash(json.url), "url" -> json.url, "summary" -> summary, "children" -> json.urls.map(el => hash(el)), "pagerank" -> pagerank)
  }

  private def hash(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DocumentConsumer").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(1))

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

    val result = stream.map(record => (record.key, record.value))

    result.foreachRDD(rdd => {
      val result = rdd.map(el => buildResult(el._2))

      EsSpark.saveToEs(result, "documents/document", Map("es.mapping.id" -> "id"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
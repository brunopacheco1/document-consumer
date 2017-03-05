package com.engsoft29.bab.spark.consumer

import java.security.MessageDigest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark.rdd.EsSpark

import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import com.amazonaws.auth.{ BasicAWSCredentials, DefaultAWSCredentialsProviderChain }
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds

object DocumentKinesisConsumer {

  private def buildResult(jsonStr: String): Map[String, Any] = {
    case class Model(url: String, document: String, urls: Array[String])
    implicit val modelFormat = jsonFormat3(Model)
    val json = jsonStr.parseJson.convertTo[Model]
    val summary = json.document.subSequence(0, Math.min(json.document.length(), 150))
    val id = hash(json.url)
    println(id)
    val pagerank = 0d

    Map("id" -> id, "url" -> json.url, "summary" -> summary, "document" -> json.document, "children" -> json.urls.map(el => hash(el)), "pagerank" -> pagerank)
  }

  private def hash(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DocumentConsumer").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(2))

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()

    val kinesisStreams = (0 until 1).map { i =>
      KinesisUtils.createStream(ssc, "bab-search-engine-consumer", "bab-search-engine", "kinesis.us-east-1.amazonaws.com", "us-east-1",
        InitialPositionInStream.LATEST, Milliseconds(2000), StorageLevel.MEMORY_ONLY)
    }

    val unionStreams = ssc.union(kinesisStreams)
    unionStreams.foreachRDD(rdd => {
      val result = rdd.map(el => buildResult(new String(el)))

      EsSpark.saveToEs(result, "documents/document", Map("es.mapping.id" -> "id"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
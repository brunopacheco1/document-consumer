package com.engsoft29.bab.spark.consumer

import java.security.MessageDigest

import scala.collection.mutable.Buffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.elasticsearch.spark.rdd.EsSpark

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import spray.json._
import DefaultJsonProtocol._

object HiveDocumentConsumer {

  private def buildResult(jsonStr: String): Map[String, Any] = {
    case class Model(url: String, title: String, document: String, documentType: String, urls: Array[String])
    implicit val modelFormat = jsonFormat5(Model)
    val json = jsonStr.parseJson.convertTo[Model]
    val summary = json.document.substring(0, Math.min(json.document.length(), 150)).replaceAll("<.?>", "").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", " ") + "..."
    val id = hash(json.url)
    println(id)
    val pagerank = 0d

    Map("id" -> id, "url" -> json.url, "title" -> json.title, "summary" -> summary, "documentType" -> json.documentType, "document" -> json.document, "children" -> json.urls.map(el => hash(el)), "pagerank" -> pagerank)
  }

  private def hash(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("HiveDocumentConsumer").setMaster("local[*]")

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder
      .config(config)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val schema = StructType(Array[StructField](StructField("father", StringType, nullable = false), StructField("child", StringType, nullable = false)))

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()

    val kinesisStreams = (0 until 1).map { i =>
      KinesisUtils.createStream(ssc, "bab-search-engine-consumer", "bab-search-engine", "kinesis.us-east-1.amazonaws.com", "us-east-1",
        InitialPositionInStream.LATEST, Milliseconds(2000), StorageLevel.MEMORY_ONLY)
    }

    val unionStreams = ssc.union(kinesisStreams)
    unionStreams.foreachRDD(rdd => {
      val documents = rdd.map(el => buildResult(new String(el)))

      val filteredDocuments = documents.filter(value => value.contains("children") && value("children").asInstanceOf[Array[String]].length > 0).flatMap(val2 => val2("children").asInstanceOf[Array[String]].map(child => (val2("id").asInstanceOf[String], child)))

      val relationships = filteredDocuments.map(el => Row(el._1, el._2))
      val relationshipsDF = spark.createDataFrame(relationships, schema)

      relationshipsDF.createOrReplaceTempView("temp_relationship")

      spark.sql("INSERT INTO TABLE relationship SELECT * FROM temp_relationship")

      EsSpark.saveToEs(documents, "documents/document", Map("es.mapping.id" -> "id"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
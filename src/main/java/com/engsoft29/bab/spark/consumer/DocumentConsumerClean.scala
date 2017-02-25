package com.engsoft29.bab.spark.consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import consumer.kafka.ReceiverLauncher
import consumer.kafka.ProcessedOffsetManager
import org.apache.spark.storage.StorageLevel

object DocumentConsumerClean {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BaBDocumentConsumer").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val topic = "documents"
    val zkhosts = "localhost"
    val zkports = "2181"

    //Specify number of Receivers you need. 
    val numberOfReceivers = 1
    val kafkaProperties: Map[String, String] =
      Map("zookeeper.hosts" -> zkhosts,
        "zookeeper.port" -> zkports,
        "kafka.topic" -> topic,
        "zookeeper.consumer.connection" -> "localhost:2181",
        "kafka.consumer.id" -> "kafka-consumer",
        //optional properties
        "consumer.fetchsizebytes" -> "1048576",
        "consumer.fillfreqms" -> "1000",
        "consumer.num_fetch_to_buffer" -> "1")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key, value) => props.put(key, value) }

    val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY)
    //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
    val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(tmp_stream, props)

    //Start Application Logic
    tmp_stream.foreachRDD(rdd => {
       rdd.take(2).foreach(el => println(new String(el.getConsumer().getBytes())))
    })
    //End Application Logic

    //Persists the Max Offset of given Kafka Partition to ZK
    ProcessedOffsetManager.persists(partitonOffset_stream, props)
    ssc.start()
    ssc.awaitTermination()
  }
}
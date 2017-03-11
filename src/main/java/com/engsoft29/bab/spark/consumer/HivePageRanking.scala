package com.engsoft29.bab.spark.consumer

import scala.collection.mutable.Buffer
import scala.util.hashing.MurmurHash3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sparkContextFunctions
import org.apache.spark.sql.SparkSession

object HivePageRanking {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().set("es.write.operation", "upsert").setAppName("HivePageRanking").setMaster("local[*]")

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder
      .config(config)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val relationships = spark.sql("SELECT father, child FROM relationship").dropDuplicates().rdd.map(row => (row(0).toString, row(1).toString))
    
    val documents = relationships.groupByKey()
    
    println("Total de documentos: " + documents.count())
    
    val edges: RDD[(VertexId, VertexId)] = relationships.map(line => (MurmurHash3.stringHash(line._1), MurmurHash3.stringHash(line._2)))

    val graph = Graph.fromEdgeTuples(edges, 1)

    val ranks = graph.pageRank(0.0001).vertices

    val vertices = documents.map(value => (MurmurHash3.stringHash(value._1).asInstanceOf[org.apache.spark.graphx.VertexId], value._1))
    
    val rankByVertices = vertices.join(ranks).map(el => Map("id" -> el._2._1, "pagerank" -> el._2._2)).filter(value => value("pagerank") != null && value("pagerank").toString.toDouble > 0)
    
    println("Total de documentos rankeados: " + rankByVertices.count())
    
    EsSpark.saveToEs(rankByVertices, "documents/document", Map("es.mapping.id" -> "id"))
  }
}
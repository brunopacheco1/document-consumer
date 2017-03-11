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

object PageRanking {
  def main(args: Array[String]): Unit = {
    val time = System.currentTimeMillis()
    
    val sc = new SparkContext(new SparkConf().set("es.write.operation","upsert").setAppName("PageRanking").setMaster("local[*]"))

    val documents = sc.esRDD("documents/document", Map[String, String]("es.read.field.include" -> "id,children,pagerank")).map(kv => kv._2)
    
    println("Total de documentos: " + documents.count())
    
    val filteredDocuments = documents.filter(value => value.contains("children") && value("children").asInstanceOf[Buffer[String]].length > 0).flatMap(val2 => val2("children").asInstanceOf[Buffer[String]].map(child => (val2("id").asInstanceOf[String], child)))

    val edges: RDD[(VertexId, VertexId)] = filteredDocuments.map(el => (MurmurHash3.stringHash(el._1), MurmurHash3.stringHash(el._2)))

    println("Total de relacionamentos: " + edges.count())
    
    val graph = Graph.fromEdgeTuples(edges, 1)

    val ranks = graph.pageRank(0.0001).vertices

    val vertices = documents.map(value => (MurmurHash3.stringHash(value("id").toString()).asInstanceOf[org.apache.spark.graphx.VertexId], value("id").toString()))
    
    val rankByVertices = vertices.join(ranks).map(el => Map("id" -> el._2._1, "pagerank" -> el._2._2)).filter(value => value("pagerank") != null && value("pagerank").toString.toDouble > 0)
    
    println("Total de documentos rankeados: " + rankByVertices.count())
    
    EsSpark.saveToEs(rankByVertices, "documents/document", Map("es.mapping.id" -> "id"))
    
    println("FIM --> " + (System.currentTimeMillis() - time))
  }
}
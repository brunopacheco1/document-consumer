package com.engsoft29.bab.spark.consumer

import scala.collection.mutable.Buffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.elasticsearch.spark._
import scala.util.hashing.MurmurHash3

object PageRanking {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("BaBPagerank").setMaster("local[*]"))

    val documents = sc.esRDD("documents/document", Map[String, String]("es.read.field.include" -> "id,children")).map(kv => kv._2)
    val filteredDocuments = documents.filter(value => value("children").asInstanceOf[Buffer[String]].length > 0).map(val2 => val2("children").asInstanceOf[Buffer[String]].map(child => (val2("id").asInstanceOf[String], child))).flatMap(identity)

    val edges: RDD[(VertexId, VertexId)] = filteredDocuments.map(el => (MurmurHash3.stringHash(el._1), MurmurHash3.stringHash(el._2)))

    val graph = Graph.fromEdgeTuples(edges, 1)

    val ranks = graph.pageRank(0.0001).vertices

    val vertices = filteredDocuments.map(el => (MurmurHash3.stringHash(el._1).asInstanceOf[org.apache.spark.graphx.VertexId], el._1)) ++ filteredDocuments.map(el => (MurmurHash3.stringHash(el._2).asInstanceOf[org.apache.spark.graphx.VertexId], el._2))

    val rankByVertices = vertices.join(ranks).map(el => el._2)
    
    rankByVertices.collect().foreach(println)

    /*val documentRanked = documents.join(rankByVertices).map(el => el._2).map(el => el._1 ++ Map("pagerank" -> el._2))

    EsSpark.saveToEs(documentRanked, "documents/document", Map("es.mapping.id" -> "id"))*/
  }
}
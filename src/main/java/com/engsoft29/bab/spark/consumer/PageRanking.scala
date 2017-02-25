package com.engsoft29.bab.spark.consumer

import org.apache.spark.SparkContext

import org.elasticsearch.spark._
import org.apache.spark.graphx._
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import org.apache.spark.SparkConf

object PageRanking {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("BaBPagerank").setMaster("spark://casa-Lenovo:7077"))

    val documents = sc.esRDD("documents/document")

    val filteredDocuments = documents.filter(linha => linha._2("children").asInstanceOf[scala.collection.mutable.Buffer[String]].length > 0).map(linha2 => linha2._2("children").asInstanceOf[scala.collection.mutable.Buffer[String]].map(child => (linha2._1, child))).flatMap(identity)

    val edges: RDD[(VertexId, VertexId)] = filteredDocuments.map(el => (MurmurHash.stringHash(el._1), MurmurHash.stringHash(el._2)))

    val graph = Graph.fromEdgeTuples(edges, 1)

    val ranks = graph.pageRank(0.0001).vertices

    val vertices = filteredDocuments.map(el => (MurmurHash.stringHash(el._1).asInstanceOf[org.apache.spark.graphx.VertexId], el._1)) ++ filteredDocuments.map(el => (MurmurHash.stringHash(el._2).asInstanceOf[org.apache.spark.graphx.VertexId], el._2))

    val rankByVertices = vertices.join(ranks).map(el => el._2)

    val documentRanked = documents.join(rankByVertices).map(el => el._2).map(el => el._1 ++ Map("pagerank" -> el._2))

    EsSpark.saveToEs(documentRanked, "documents/document", Map("es.mapping.id" -> "id"))
  }
}
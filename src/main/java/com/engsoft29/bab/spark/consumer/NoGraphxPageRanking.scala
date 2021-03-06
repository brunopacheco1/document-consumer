package com.engsoft29.bab.spark.consumer

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sparkContextFunctions

object NoGraphxPageRanking {
    
  def main(args: Array[String]): Unit = {
    val time = System.currentTimeMillis()
      
    val sparkConf = new SparkConf().setAppName("NoGraphxPageRank")
    
    val iters = 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile("relationship.txt")
    
    println("Total de relacionamentos: " + lines.count())
    
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val documentsToSave = ranks.map(value => Map("id" -> value._1, "pagerank" -> value._2))
    
    println("Total de documentos rankeados: " + documentsToSave.count())
    
    EsSpark.saveToEs(documentsToSave, "documents/document", Map("es.mapping.id" -> "id"))

    ctx.stop()
    
    println("Tempo total gasto: " + (System.currentTimeMillis() - time) + "ms")
  }
}
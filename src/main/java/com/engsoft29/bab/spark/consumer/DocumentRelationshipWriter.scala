package com.engsoft29.bab.spark.consumer

import scala.collection.mutable.Buffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.spark.sparkContextFunctions

object DocumentRelationshipWriter {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("DocumentRelationshipWriter").setMaster("local[*]");
    val spark = SparkSession.builder().config(config).getOrCreate()

    val schema = StructType(Array[StructField](StructField("father", StringType, nullable = false), StructField("child", StringType, nullable = false)))

    case class Relationship(father: String, child: String)

    val documents = spark.sparkContext.esRDD("documents/document", Map[String, String]("es.read.field.include" -> "id,children")).map(kv => kv._2)
    val filteredDocuments = documents.filter(value => value.contains("children") && value("children").asInstanceOf[Buffer[String]].length > 0).map(val2 => val2("children").asInstanceOf[Buffer[String]].map(child => (val2("id").asInstanceOf[String], child))).flatMap(identity)

    val relationships = filteredDocuments.map(el => Row(el._1, el._2))
    val relationshipsDF = spark.createDataFrame(relationships, schema)
    relationshipsDF.write.parquet("spark-warehouse/relationship")
  }
}
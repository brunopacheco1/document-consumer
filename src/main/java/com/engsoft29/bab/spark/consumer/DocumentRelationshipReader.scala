package com.engsoft29.bab.spark.consumer

import scala.collection.mutable.Buffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.spark.sparkContextFunctions

object DocumentRelationshipReader {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("DocumentRelationshipReader").setMaster("local[*]");
    val spark = SparkSession.builder().config(config).getOrCreate()

    val schema = StructType(Array[StructField](StructField("father", StringType, nullable = false), StructField("child", StringType, nullable = false)))

    val relationshipDF = spark.read.parquet("spark-warehouse/relationship")
    
    println(relationshipDF.count())
    
    val result2 = relationshipDF.dropDuplicates()
    println(result2.count())
  }
}
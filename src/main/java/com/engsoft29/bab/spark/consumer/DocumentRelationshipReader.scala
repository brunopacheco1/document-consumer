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

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder
      .config(config)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    val result = spark.sql("SELECT * FROM relationship")

    println(result.count())
    
    println(result.dropDuplicates().count())
  }
}
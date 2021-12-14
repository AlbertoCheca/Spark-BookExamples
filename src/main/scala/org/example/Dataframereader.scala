package org.example

import org.apache.spark.sql.SparkSession

object Dataframereader {
  def main(args : Array[String]) {
    val spark = SparkSession
      .builder
      .appName("dataframeReader")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // In Scala
    // Use Parquet
    val file = """/databricks-datasets/learning-spark-v2/flights/summary-
 data/parquet/2010-summary.parquet"""
    val df = spark.read.format("parquet").load(file)
    // Use Parquet; you can omit format("parquet") if you wish as it's the default
    val df2 = spark.read.load(file)
    // Use CSV
    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")
    // Use JSON
    val df4 = spark.read.format("json")
      .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

  }
}

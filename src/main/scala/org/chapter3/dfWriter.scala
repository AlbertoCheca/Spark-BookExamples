package org.chapter3

import org.apache.spark.sql.SparkSession

object dfWriter {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")



    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load(args(0))

    df3.write
      .format("csv")
      .mode("overwrite")
      .save("/tmp/data/csv/df_csv")

    df3.write
      .format("json")
      .mode("overwrite")
      .save("/tmp/data/json/df_json")

    df3.write
      .format("avro")
      .mode("overwrite")
      .save("/tmp/data/avro/df_avro")

  }
}

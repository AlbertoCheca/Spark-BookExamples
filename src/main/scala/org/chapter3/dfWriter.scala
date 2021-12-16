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


  }
}

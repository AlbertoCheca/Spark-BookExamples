package org.chapter3
import org.apache.spark.sql.SparkSession

object csvReader {
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

    print(df3.printSchema())

  }
}

package org.example

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

object RDDexample {
  def main(args : Array[String]) {
    val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // Create a DataFrame of names and ages
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
    // Group the same names together, aggregate their ages, and compute an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))
    // Show the results of the final execution
    avgDF.show()
  }
}

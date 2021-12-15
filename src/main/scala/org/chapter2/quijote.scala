package org.chapter2

import org.apache.spark.sql.SparkSession

object quijote {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("dataframeReader")
      .config("spark.master", "local")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")


    val elquijote = args(0)

    // Read the file into a RDD
    val qRDD = spark.sparkContext.textFile(elquijote)
    //mostramos por pantalla cuantas lineas tiene
    Console.println(qRDD.count())

  }
}

package org.padron

import org.apache.spark.sql.SparkSession

object PadronReader {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("PadronReader")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")



    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load(args(0))

/*    df3.write
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
      .save("/tmp/data/avro/df_avro")*/


  }
}

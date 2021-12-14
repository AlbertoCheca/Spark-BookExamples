package org.chapter4

import org.apache.spark.sql.SparkSession

object Aeropuertos {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("dataframeReader")
      .config("spark.master", "local")
      .config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

    spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS\n SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE\n origin = 'SFO'")
    spark.sql("CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS\n SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE\n origin = 'JFK'")
    spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")



  }
}

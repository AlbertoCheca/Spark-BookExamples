package org.padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc}

object PadronReader {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("PadronReader")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


//cargamos el archivo que pasamos por argumentos
    val padronDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("delimiter",";")
      .load(args(0))

   // padronDF.na.fill(0)

    padronDF.show(10)
    //Total de lineas
    println(s"Total Rows = ${padronDF.count()}")
    //enmurar barrios diferentes
   /* val BarriosDF = padronDF
      .select("DESC_BARRIO").distinct()

    BarriosDF.show()*/

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

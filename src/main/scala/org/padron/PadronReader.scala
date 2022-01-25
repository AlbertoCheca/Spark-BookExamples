package org.padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{count, desc, col}


object PadronReader {

  def main(args: Array[String])
  {

    val spark = SparkSession
      .builder()
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

    padronDF.na.fill(0).show(false)


    //Total de lineas

//----------------------------------------------------------
  /*  //enmurar barrios diferentes
    val BarriosDF = padronDF
      .select("DESC_BARRIO").distinct()

    println(s"Numero de barrios = ${BarriosDF.count()}")
    BarriosDF.show()*/
//---------------------------------------------------------------------------
   /* //contar el numero de barrios con tabla temporal

    padronDF.createOrReplaceTempView("tempdf")


    val sqlDF = spark.sql("SELECT * FROM tempdf")

    println(s"Numero de barrios = ${sqlDF.count()}")*/
//------------------------------------------------------------------------
    //añadir columna nueva

    padronDF.withColumn("longitud",padronDF.col("DESC_DISTRITO"))

    padronDF.take(5)
    println(s"Total Rows = ${padronDF.count()}")
  }
}

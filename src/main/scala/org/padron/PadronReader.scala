package org.padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.expr
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

   // padronDF.na.fill(0).show(false)


    //Total de lineas
    println(s"Total Rows = ${padronDF.count()}")
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
/*
    //añadir columna nueva con longitud de DESC_DISTRITO

    padronDF.withColumn("longitud",expr("length(DESC_DISTRITO)")).show(10)

*/

    //------------------------------------------------------------------------
/*
    //Columna nueva con 5s


 val columnaDF = padronDF.withColumn("cinco",expr("5"))

    columnaDF.show(10)
    //------------------------------------------------------------------------
   // Borrar la columna

    val dropColDF = columnaDF.drop("cinco").show(10)*/

    //-----------------------------------------------------------------------
/*
    //crear una particion y almacenarla


    padronDF.write
      .format("csv")
      .mode("overwrite")
      .partitionBy({"DESC_DISTRITO";"DESC_BARRIO"})
      .save("/tmp/data/csv/df_csv")*/

    //-----------------------------------------------------------------------
    val descDF = padronDF.select("DESC_BARRIO","DESC_DISTRITO","EspanolesHombres")

   descDF.show(10)
/*

    padronDF
      .join(descDF, $"padronDF.DESC_BARRIO" === $"descDF.DESC_BARRIO")
      .show(10)
*/


  }
}

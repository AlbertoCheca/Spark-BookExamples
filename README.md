# Spark-LearningSparkBook
Ejercicios del libro Learning Spark **Lightning-Fast Data Analytics** 

####Examples
En el paquete examples tengo los ejemplos del libro  modificados ligeramente 

####Chapter 1 :Introduction to Apache Spark
El primer capitulo es más teorico y se centra en entender como funciona Spark 

####Chapter 2 :Getting started
En este capitulo tenemos el primer ejercicio con los MnM en el que los contamos ordenamos y clasificamos por colores 
Tambien esta el ejercicio de El Quijote en el que realizamos una serie de consultas al .txt del libro
####Chapter 3 :Structured APIs
Aqui usamos readers y writers para manejar los archivos en distintos formatos como JSON,CSV o AVRO 

#####Preguntas
 + Cuando se define un schema al definir un campo por ejemplo StructField('Delay', 
   FloatType(), True) ¿qué significa el último parámetro Boolean?
   
Si el campo puede ser nulo o no
 + Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código? 
 
  Dataset: es un conjunto de datos tipados, mientras que DataFrame es un conjunto de Datasets sin tipar. Para definir un Dataset, al ser un conjunto de datos enorme, inferir el schema puede llegar a ser costoso
por ello se utiliza "case class" para definir una clase con todos los tipos de cada columna del dataset


 
 Mas informacion en https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html#



####Chapter 4 :SparkSQL & Dataframes
En este capitulo trabajamos con los dataframes y hacemos consultas de SparkSQL
#####Preguntas
+ GlobalTempView vs TempView

Las TempView son dependientes de la SparkSession en la que estamos, y cuando acabe se destruiran, mientras que las GlobalTempView estan activas mientras la apliacion esta activa

####Chapter 5 :Spark SQL and DataFrames: Interacting with External Data Sources
En este capitulo trabajamos con los dataframes y veremos como interactuan cono otras herramientas como Hive
#####Preguntas
+ Pros y Cons utilizar UDFs 
 
 +Permite encapsular código y reusarlo, lo que se traduce como menor tiempo de desarrollo 
 
 +Es posible rastrear fácilmente e identificar el lugar en el que se aplican las reglas de negocio reusables
 
 -Se almacenan en el proyecto, y hanroa que repetirlos si hacemos otro
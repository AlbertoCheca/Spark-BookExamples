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
# Práctica Hive + Impala + HDFS + Spark Padron

## 1- Creación de tablas en formato texto.
### 1.1) Crear Base de datos "datos_padron" .
creamos la base de datos 


create database datos_padron;
use datos_padron;
### 1.2)Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los datos 


create table padron_txt(
COD_DISTRITO int,
DESC_DISTRITO string,
COD_DIST_BARRIO int,
DESC_BARRIO string,
COD_BARRIO int,
COD_DIST_SECCION string,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "\;", "quoteChar" = "\"")
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/home/cloudera/ejercicios/padron/padron.csv" INTO TABLE padron;

### 1.6)Regex  

create table if not exists padron_txt1(
COD_DISTRITO int,
DESC_DISTRITO string,
COD_DIST_BARRIO int,
DESC_BARRIO string,
COD_BARRIO int,
COD_DIST_SECCION int,
COD_SECCION int,
COD_EDAD_INT int,
EspanolesHombres int,
EspanolesMujeres int,
ExtranjerosHombres int,
ExtranjerosMujeres int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex'='"(.*)";"([A-Za-z]*) *";"(.*)";"([A-Za-z]*) *";"(.*)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)"')
stored as textfile
tblproperties("skip.header.line.count"="1");



LOAD DATA LOCAL INPATH "/home/cloudera/ejercicios/padron/padron.csv" INTO TABLE padron_txt1;

## 2- Investigamos el formato columnar parquet
 
### 2.1) ¿Qué es CTAS?

CTAS (Create Table As...) 
Es una operacion que crea una tabla particionada a partir de una tabla normal en una base de datos, copia las restricciones como NULL,NOT NULL, etc sin embargo no coopia los valores por defecto lo cuyal puede dar errores en los que  haya valores nulos donde no deberia.

Para solucionar esto se puede modificar la tabla haciendo un ALTER TABLE o cambiar el codigo de la aplicacion para que tenga un valor por defecto  

### 2.2) Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla padron_txt mediante un CTAS.

CREATE TABLE particion_padron STORED AS PARQUET AS SELECT*  FROM padron_txt;

### 2.3)Crear tabla Hive padron_parquet_2 a través de la tabla padron_txt_2 mediante un CTAS

CREATE TABLE particion_padron_2 STORED AS PARQUET AS SELECT*  FROM padron_txt_2;

### 2.5) Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar con este tipo de formatos.
+ autodescrito 
+ formato de columnas
+ independiente del lenguaje

Parquet almacena filas mientras que  Avro almacena en columnas. Si necestiamos trabajar con todos los datos de una fila en una query Avro es la mejor opcion. 

Si el dataset tiene muchas columnas y trabajamos con un subset de ellas Parquet esta optimizado para esto.

En general trabajar con Parquet da resultados similares o mejores a trabajar con Avro.
## 3- Juguemos con Impala.

###  3.1)¿Qué es Impala?
Impala ofrece una tecnología de base de datos escalable y paralela sobre Hadoop permitiendo a los usuarios realizar consultas SQL con baja latencia sobre los datos guardados en el HDFS o en HBase sin necesidad de moverlos o transformarlos. Impala esta integrado con Hadoop para que utilice los mismos ficheros, formatos, metadatos, seguridad y frameworks de administración de recursos utilizados por MapReduce, Apache Hive, Apache Pig, etc.

### 3.2) ¿En qué se diferencia de Hive?
La velocidad de procesamiento de consultas en Hive es lenta, pero Impala es de 6 a 69 veces más rápida que Hive. En Hive, la latencia es alta, pero en Impala, la latencia es baja. Hive admite el almacenamiento de archivos RC y ORC, pero el almacenamiento de Impala es compatible con Hadoop y Apache HBase.

### 3.3)  Comando INVALIDATE METADATA, ¿en qué consiste?
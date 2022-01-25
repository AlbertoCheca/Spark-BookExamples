# Práctica Hive + Impala + HDFS + Spark Padron

## 1- Creación de tablas en formato texto.
### 1.1) Crear Base de datos "datos_padron" .
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
    marca los metadatos de una tabla como stale. La proxima vez que impala realice una query, Impala recarga los metadatos asociados antes de realizar la query. es una operacion mas costosa que actualizar los metadatos mediante REFRESH, la cual es mas recomendable

### 3.4) Hacer invalidate metadata en Impala de la base de datos datos_padron.

    INVALIDATE METADATA datos_padron;

### 3.5) Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.

    SELECT SUM(espanoleshombres),SUM(espanolesmujeres),SUM(extranjeroshombres),SUM(extranjerosmujeres) FROM padron_txt
      GROUP BY desc_distrito,desc_barrio ;

### 3.6)Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2 
### 3.7) Llevar a cabo la misma consulta sobre las mismas tablas en Impala. 
### 3.8) ¿Se percibe alguna diferencia de rendimiento entre Hive e Impala?
    Si , Impala es muchisimo mas rapido que Hive


## 4- Sobre tablas particionadas.

### 4.1) Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.

    CREATE TABLE datos_padron.padron_particionado(COD_DISTRITO INT, COD_DIST_BARRIO INT,
    COD_BARRIO INT, COD_DIST_SECCION INT,
    COD_SECCION INT, COD_EDAD_INT INT,
    EspanolesHombres INT, EspanolesMujeres INT,
    ExtranjerosHombres INT, ExtranjerosMujeres INT)
    PARTITIONED BY(DESC_DISTRITO STRING, DESC_BARRIO STRING)
    STORED AS PARQUET;

### 4.2)Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla padron_parquet_2.

    FROM datos_padron.padron_parquet_2
    INSERT OVERWRITE TABLE datos_padron.padron_particionado
    PARTITION(DESC_DISTRITO, DESC_BARRIO)
    SELECT COD_DISTRITO, COD_DIST_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT,
    EspanolesHombres,EspanolesMujeres, ExtranjerosHombres,ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO;

### 4.3) Hacer invalidate metadata en Impala de la base de datos padron_particionado.

    INVALIDATE METADATA padron_particionado;

### 4.4)Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.

    select desc_distrito,desc_barrio,sum(espanoleshombres),

    sum(espanolesmujeres),sum(extranjeroshombres),sum(extranjerosmujeres)

    from padron_txt

    where desc_distrito LIKE "CENTRO" or desc_barrio Like "LATINA" or desc_barrio like "CHAMARTIN" or desc_barrio like "TETUAN"

    group by desc_distrito,desc_barrio;

### 4.5)Llevar a cabo la consulta en Hive en las tablas padron_parquet y padron_partitionado.
### 4.6)Llevar a cabo la consulta en Impala en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?

    Impala va mucho mas rapido que hive


## 5- Trabajando con tablas en HDFS

### 5.1)Crear un documento de texto en el almacenamiento local que contenga una secuencia de números distribuidos en filas y separados por columnas, llámalo datos1 y que sea por ejemplo:
    1,2,3

    4,5,6

    7,8,9

### 5.2) Crear un segundo documento (datos2) con otros números pero la misma estructura

### 5.3)Crear un directorio en HDFS con un nombre a placer, por ejemplo, /test. Si estás en una máquina Cloudera tienes que asegurarte de que el servicio HDFS está activo ya que puede no iniciarse al encender la máquina (puedes hacerlo desde el Cloudera Manager). A su vez, en las máquinas Cloudera es posible (dependiendo de si usamos Hive desde consola o desde Hue) que no tengamos permisos para crear directorios en HDFS salvo en el directorio /user/cloudera.

    hadoop fs -mkdir /home/cloudera/ejercicios/test3

### 5.4)Mueve tu fichero datos1 al directorio que has creado en HDFS con un comando desde consola.

     hadoop fs -put /home/cloudera/Desktop/datos1 /home/cloudera/ejercicios/test3

### 5.5)Desde Hive, crea una nueva database por ejemplo con el nombre numeros. Crea una tabla que no sea externa y sin argumento location con tres columnas numéricas, campos separados por coma y delimitada por filas. La llamaremos por ejemplo numeros_tbl

    CREATE TABLE numeros_tbl(numero1 INT,numero2 INT ,numero3 INT )ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ("separatorChar" = "\,", "quoteChar" = "\"");

### 5.6)Carga los datos de nuestro fichero de texto datos1 almacenado en HDFS en la tabla de Hive. Consulta la localización donde estaban anteriormente los datos almacenados. ¿Siguen estando ahí? ¿Dónde están?. Borra la tabla, ¿qué ocurre con los datos almacenados en HDFS?

    LOAD DATA INPATH "/home/cloudera/ejercicios/test3/datos1" INTO TABLE numeros_tbl;

---
    hadoop fs -ls /home/cloudera/ejercicios/test3
---
    los datos ya no aparecen , solo esta el datos2 los datos ahora estan en la tabla de hive , al borrrar la tabla los datos tambien se borran

### 5.7)Vuelve a mover el fichero de texto datos1 desde el almacenamiento local al directorio anterior en HDFS.
    hadoop fs -put /home/cloudera/Desktop/datos1 /home/cloudera/ejercicios/test3


### 5.8)Desde Hive, crea una tabla externa sin el argumento location. Y carga datos1 (desde HDFS) en ella. ¿A dónde han ido los datos en HDFS? Borra la tabla ¿Qué ocurre con los datos en hdfs?

    CREATE EXTERNAL TABLE ext_numeros_tbl(numero1 INT,numero2 INT ,numero3 INT )ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ("separatorChar" = "\,", "quoteChar" = "\"");

    LOAD DATA INPATH "/home/cloudera/ejercicios/test3/datos1" INTO TABLE ext_numeros_tbl;
---
    los datos ya no aparecen , solo esta el datos2 los datos ahora estan en la tabla de hive , al borrrar la tabla los datos tambien se borran

### 5.9)Borra el fichero datos1 del directorio en el que estén. Vuelve a insertarlos en el directorio que creamos inicialmente (/test). Vuelve a crear la tabla numeros desde hive pero ahora de manera externa y con un argumento location que haga referencia al directorio donde los hayas situado en HDFS (/test). No cargues los datos de ninguna manera explícita. Haz una consulta sobre la tabla que acabamos de crear que muestre todos los registros. ¿Tiene algún contenido?


### 5.10)Inserta el fichero de datos creado al principio, "datos2" en el mismo directorio de HDFS que "datos1". Vuelve a hacer la consulta anterior sobre la misma tabla. ¿Qué salida muestra? 


### 5.11)Extrae conclusiones de todos estos anteriores apartados

    Al modificar los archivos en hive pueden llegar a perderse los originales en hdfs al destruir tablas

## 6- Un poquito de Spark.

### 6.1)Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema.

    val padronDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("delimiter",";")
      .load(args(0))

    padronDF.na.fill(0).show(false)

### 6.2)De manera alternativa también se puede importar el csv con menos tratamiento en la importación y hacer todas las modificaciones para alcanzar el mismo estado de limpieza de los datos con funciones de Spark.

### 6.3)Enumera todos los barrios diferentes.
    val BarriosDF = padronDF
    .select("DESC_BARRIO").distinct()

    println(s"Numero de barrios = ${BarriosDF.count()}")
    BarriosDF.show()
### 6.4)Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barriosdiferentes que hay.
    padronDF.createOrReplaceTempView("tempdf")

    val sqlDF = spark.sql("SELECT * FROM tempdf")

    println(s"Numero de barrios = ${sqlDF.count()}")
### 6.5)Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".
### 6.6)Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla. 

### 6.7)Borra esta columna.

### 6.8)Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.

### 6.9)Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados.

### 6.10)Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".

### 6.11)Elimina el registro en caché.

### 6.12)Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a través de las columnas en común.

### 6.13)Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).

### 6.14)Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) quecontenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a este:

### 6.15)Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.

### 6.16)Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.

### 6.17)Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.

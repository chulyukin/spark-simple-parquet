# spark-simple-parquet 
Репозиторий примера простого проекта **Spark 3.5.6** (сборщик **Scala SBT assembly**). 

## Overview
Это демонстрационный проект **Scala** **Spark** для сборщика **SBT assembly** (**«fat» jar**), с примером простого кода, для создания приложения. Приложение запускается в среде фреймворка **Spark** (spark-submit). Данный пример предназначен для использования в образовательных целях или как основа для других проектов **Scala Spark**. Содержит минимально необходимую структуру для сборки исполняемого приложения **java --jar**. В описании к проекту приведены  основные пояснения настроек и кода.  Для проекта используется локально установленный **Spark ver.3.5.6**. 

## Essence
Приложение — конвертер файлов формата **parquet** в формат **csv**, с возможностью выгрузки сэмпла данных. Производится чтение .parquet файлов по пути, указанному в параметре запуске, конвертация исходных файлов формат **csv** (разделитель «;») и сохранение результата по пути, указанному в параметре запуска. В отдельном параметре указывается размер сэмпла (количество строк), с возможностью значения «all», при котором результирующий csv будет содержать все строки исходного файла parquet. Дополнительно, для каждого конвертируемого файла формируется простая статистика по исходному файлу (.parquet) (количество записей, структура и размер исходного файла).  

**Производятся следующие шаги:**
 - создание sparк сессии (согласно переданной конфигурации в spark-submit);
 - чтение parquet файлов по заданному пути в параметрах;
 - формирование статистик по parquet;
 - преобразование parquet файлов в формат csv с учетом параметра сэмпла;
 - сохранение csv — сэмпла и статистики по заданному пути в параметрах.

## Requirements
|Инструмент|Версия|Комментарий|Ресурс|
|:-|:-|:-|:-|
|Java|openjdk-17|Openjdk-17 - это (Open Java Development Kit) - это бесплатная реализация платформы Java Standard Edition (Java SE) с открытым исходным кодом |https://openjdk.org/|
|Scala|2.12.20|Проверенная стабильная версия, хорошо совместима с OoenJDK-17 (в качестве альтернативы можно использовать 2.12.18 )|https://scala-lang.org/|
|SBT|1.11.2| Scala build tool. В качестве альтернативы можно использовать не ниже 1.6.1|https://docs.scala-lang.org/overviews/scala-book/scala-build-tool-sbt.html|  

Про организацию рабочей среды, в которой собирается данный проект, можно прочитать здесь: https://github.com/chulyukin/howto-base-notes)

## Assembly and Launch
1) Загрузить проект в директорию проектов SBT (_например в ~/SBTProjects_):
```console
cd ~/SBTProjects
git clone https://github.com/chulyukin/spark-simple-parquet.git
```
2) Перейти в директорию с проектом и выполнить:
```console
cd ~/SBTProjects/spark-simple-parquet
sbt
# [info] welcome to sbt 1.11.2 (Ubuntu Java 17.0.14)
# [info] loading settings for project spark-simple-parquet-build from plugins.sbt...
# [info] started sbt server
sbt:sparkparquet> 
```
3) Далее выполнить команду сборки
```console
assembly
```
В директории target/scala-2.12, будет собран файл **sparkparquet-assembly-0.1.1.jar**
```console
ls ~/SBTProjects/spark-simple-parquet/target/scala-2.12
# classes sparkparquet-assembly-0.1.1.jar  sync  update  zinc
```
4) Запуск **sparkparquet-assembly-0.1.1.jar** на исполнение

|Параметр|Описание|Пример|
|:-|:-|:-|
|--source-dir| Строка. Путь к директории файлов .parquet для конвертирования|--source-dir example_parquet|
|--output-dir| Строка. Путь для сохранения результирующих файлов .csv. Указанная директория будет создана автоматически, в случае отсутствия|--output-dir output|
|--sample-size| Строка **"all"** или число больше 0. Количество выгружаемых строк в сэмпл .csv. Если задано значение **"all"**, будут выгружены все строки из исходного файла|--sample-size all|

```console
spark-submit \
--master local[2] \
--class com.simple.sparkparquet.SparkParquetMain \
~/SBTProjects/spark-simple-parquet/target/scala-2.12/sparkparquet-assembly-0.1.1.jar \
--source-dir example_parquet \
--output-dir output \
--sample-size 30
```
Во время выполнения, задача отображается в пользовательском интерфейсе мастера Spark. Пользовательский интерфейс можно просматривать по адресу localhost:4040. http://localhost:4040/jobs/ 

5) Результат (в случае запуска на тестовых источниках _example_parquet_)
```console
ls example_parquet/
# yellow_tripdata_2023-08.parquet  yellow_tripdata_2023-10.parquet
```
Будет создана директория output с файлами .csv
```console
ls output/
# yellow_tripdata_2023-08.csv  yellow_tripdata_2023-08.txt  yellow_tripdata_2023-10.csv  yellow_tripdata_2023-10.txt
```
Пример отчета статистики файла .parquet
```console
cat output/yellow_tripdata_2023-08.txt
```
```text
file: yellow_tripdata_2023-08.parquet
size (kB): 47023
count rows: 2824209
root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
 |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
 |-- passenger_count: long (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: long (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- Airport_fee: double (nullable = true)
```
## Project architecture
Структура кода
```tree
.
├── build.sbt
├── example_parquet
│   ├── yellow_tripdata_2023-08.parquet
│   └── yellow_tripdata_2023-10.parquet
├── LICENSE
├── project
│   ├── build.properties
│   └── plugins.sbt
├── README.md
└── src
    └── main
        ├── resources
        │   └── log4j2.properties
        └── scala
            └── com
                └── simple
                    └── sparkparquet
                        ├── package.scala
                        ├── Processing.scala
                        └── SparkParquetMain.scala
```
**Файловая структура в src/main/scala/com/simple/**  
|Файл|Описание|
|:-|:-|
|package.scala| Файл определения пакетного объекта package object sparkparquet. Это контейнер для функций, переменных и псевдонимов типов, которые доступны в элементах дочернего пакета com.simple.sparkparquet по прямому доступу (без импорта). |
|Processing.scala| Объект - обработчик с функцией processor. В данном объекте реализуется функционал обработки, вызываемый в main - объекте (SparkParquetMain) |
|SparkParquetMain.scala| Точка входа в программу - object SparkParquetMain extends App |  

Код организован следующим образом:
  - объекты общего назначения находятся в src/main/scala/com/simple/sparkparquet/package.scala в package object sparkparquet
  - объекты - обработчики (в данном случае создание сэмпла и собр статистики ) в src/main/scala/com/simple/sparkparquet/Processing.scala
  - основной объект вызова в src/main/scala/com/simple/sparkparquet/SparkParquetMain.scala
    
При запуске, вызывается object SparkParquetMain extends App (из SparkParquetMain.scala), в котором последовательно вызываются оъекты функционала обработки из object Processing (файл Processing.scala).  
При добавлении собственных, вызываемых в **object extends App*, обработчиков - рекомендуется это делать именно в **object Processing** файла SparkParquetMain.scala. 
Такой подход считается удобным для небольших приложений и приложений среднего размера, реализуемых на **Scala**

## Example test parquet
Данные для тестового примера находятся в директории  example_parquet
Для примера выложены два файла .parquet: 
  - yellow_tripdata_2023-08.parquet
  - yellow_tripdata_2023-10.parquet

Источник данных — TLC Trip Record Data.  
TLC Trip Record Data — это набор данных, содержащий подробную информацию о поездках на такси в Нью-Йорке. Данные были загрузжены с [сайта https://www.nyc.gov](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page?source=post_page-----80e2680d3528--------------------------------). Это записи о поездках на желтом такси (Yellow Taxi Trip).

```markdown
Источник данных

Данный репозиторий содержит файлы формата Parquet, созданные на основе открытого набора данных о поездках такси и автомобилей с водителем (TLC Trip Record Data) Нью-Йорка.

Источник: 
NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Лицензия: 
Данные предоставлены городом Нью-Йорк в рамках политики открытых данных и находятся в публичном достоянии (Public Domain). Подробнее см. NYC Open Data Terms of Use.
```
## External links
[Scala language](https://scala-lang.org)  
[Scala Book.The most used scala build tool (sbt)](https://docs.scala-lang.org/overviews/scala-book/scala-build-tool-sbt.html)  
[Git.Рабочая среда. Установка и конфигурирование.](https://github.com/chulyukin/howto-base-notes)  
[TLC Trip Record Data. Данные для теста в формате parquet](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
[Spark Essentials: Руководство по настройке и запуску проектов Spark с помощью Scala и sbt](https://habr.com/ru/companies/otus/articles/840362/)  
[Как устранить ошибку "java.nio.file.NoSuchFileException: xxx/hadoop-client-api-3.3.4.jar" в Spark при запуске "sbt run`?](https://stackoverflow.com/questions/76059598/how-to-resolve-harmless-java-nio-file-nosuchfileexception-xxx-hadoop-client-ap)  
[Зачем нужно добавлять "fork:=true" при запуске приложения Spark SBT?](https://stackoverflow.com/questions/44298847/why-do-we-need-to-add-fork-in-run-true-when-running-spark-sbt-application/)
[Git.Spark/conf/log4j2.properties.template](https://github.com/apache/spark/blob/master/conf/log4j2.properties.template/)  
[Creating a Fat JAR Using SBT](https://www.baeldung.com/scala/sbt-fat-jar/)  
[Как использовать файл log4j2.properties в приложении Spark?](https://stackoverflow.com/questions/77214410/)  
[Git. Spark sbt Tutorial](https://github.com/SA01/spark-sbt-tutorial/tree/main/)

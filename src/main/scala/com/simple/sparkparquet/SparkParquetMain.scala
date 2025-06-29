package com.simple.sparkparquet

import com.simple.sparkparquet.Processing.processor
import java.io.File

object SparkParquetMain extends App {
  // Чтение парамтеров
  val (inputPath, outputPath, sampleSize) = parseArgs(args = args)
  // Инициализация Spark session
  val spark = createSparkSession("Spark test-convertor")
  // Чтение файлов .parquet
  val parquetFiles = new File(inputPath)
                            .listFiles()
                            // Фильтр по расширению
                            .filter(_.getName.endsWith(".parquet"))
                            // Имя файла и размер в килоБайтах
                            .map(file => (file.getName,file.length()))
  // Обработка файлов по отдельности
  parquetFiles.foreach {
    case (filename, size) =>
        // Чтение parquet
        val data = spark.read.parquet(s"${inputPath}/${filename}")
 // Обработка данных и статистика
        val (processedDF, stat) = processor(data = data
                                           ,limit_rows = sampleSize
                                           ,file = (filename, size)
                                           )   
 // Сохранение в spark csv            
        processedDF.coalesce(1).write
                               .mode("overwrite")
                               .option("header", "true")
                               .option("delimiter", ";")
                    .csv(s"${outputPath}/${filename}")
  // Копирование файла в основную директорию и переименование 
        copyRenameCsvFiles(outputPath, filename)
  // Сохранение статистики
        saveTextFile (path = outputPath, file = filename, text = stat)
                                      }
  // Удаление исходных папок
  removeFolders(pathToDelete = outputPath)
}

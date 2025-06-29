package com.simple
/* package.scala - package object, с функцией для создания сессии Spark и общими функциями для прямого вызова из пакета com.simple.sparkparquet
Создается package object sparkparquet с функциям 
 - createSparkSession
 - parseArgs
 - copyRenameCsvFiles
 - removeFolders
 - saveTextFile
*/
import java.io.File
import java.nio.file.{Files,Paths,StandardCopyOption}
import java.util.Comparator
import org.apache.spark.sql.SparkSession

package object sparkparquet {

/* Фукция создания SparkSession. 
Параметры:
  appName: String - Имя приложения (имя которое будет отображаться в Application Name в пользовательскому интерфейсе Spark)
При вызове spark-submit, передаются параметры конфигурации spark сессии, данная функция создает сессию с именем appName и
переданной конфигурацией в spark-submit
*/
  def createSparkSession(appName: String): SparkSession = {
      SparkSession
        .builder()
        .config("spark.sql.caseSensitive", value = true)
        .config("spark.sql.session.timeZone", value = "UTC")
        .appName(appName)
        .getOrCreate()
  }

/* Функция разбора аргументов из командной строки
Parameters:
    Array[String] - переданные аргументы при вызове
Returns: 
    sourceDir: String   - директория источник (из --source-dir)
    outputDir: String   - целевая директория  (из --output-dir)
    sampleSize: String  - количество строк в выгружаемом сэмпле (или -1, если нужно выгрузить все строки) (из --sample-size)
*/
  def parseArgs(args: Array[String]): (String, String, Int) = {
    val Usage = "Usage params: [--source-dir <str value>] [--output-dir <str value>] [--sample-size <int value or string all>]"
    if (args.length<3) println(Usage)
    // Перекладывание пар аргументов в Map (key: имя параметра. value - значение параметра)  
    val argsMap = args.map(x=>x).grouped(2).map(x=>(x(0),x(1))).toMap
    val sourceDir = argsMap("--source-dir")
    val outputDir = argsMap("--output-dir")
    val sampleSizePar = argsMap("--sample-size")
    val sampleSize: Int = if (sampleSizePar == "all") -1 else sampleSizePar.toInt

    println(s"input path: ${sourceDir}")
    println(s"output path: ${outputDir}")
    println(s"sample-size: ${sampleSizePar}")
    
    (sourceDir, outputDir, sampleSize)
  }

 
/* Функция переименования единичного файла
Parameters:
   parPath: String - основной путь, в котором сохраняются файлы csv
   filename: String - имя файла csv, созданного saprk dataframe - write - csv
Returns: 
  - нет
При вызове spark dateframe.coalesce(1)...write... csv, Spark создает для каждого сохраняемого обьекта отдельную директорию, 
с именем сохраняемого csv, в которой будет сохранен файл part-<нечитаемая строка>.csv
Данная функция производит поиск сохраненных файлов part-<нечитаемая строка>.csv, переименовывает их в целевое название и
копирует из директории в основную папку
*/
 def copyRenameCsvFiles(parPath: String, filename: String): Unit = {
    // Чтение всех файлов .csv в директории по пути из parPath/filename
    val dir = new File(s"${parPath}/${filename}")
    // Поиск файлов .csv, которые имеют префикс part-
    val partFile = dir.listFiles().find(f => f.isFile && f.getName.startsWith("part-") && f.getName.endsWith(".csv") )
    // Копирование и переименование
    partFile match {
      case Some(pFile) => 
        val targetPath = Paths.get(parPath, s"${filename.replaceAll("\\.[^.]*$", "")}.csv")
        Files.copy(pFile.toPath, targetPath, StandardCopyOption.REPLACE_EXISTING)
      case None => println("Файлы *.parquet не найдены")
                  }
   }

/* Функция удаления директорий с фалами по переданному в параметре pathToDelete пути
Parameters:
   pathToDelete: String - путь для удаления всех зависимых папок и файлов 
Returns: 
  - нет
По пути переданному в параметре pathToDelete, удаляются все файлы и директории, при этом сам путь pathToDelete
не удаляется
 */
 def removeFolders( pathToDelete: String ): Unit = 
  Files.list(Paths.get(pathToDelete)).forEach{ path => 
              if (Files.isDirectory(path)) Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete) } 
  
/* Функция сохранения текстового файла 
Parameters:
  path: String - путь для сохранения файла 
  file: String - имя сохраняемого файла 
  text: String - текст, который требуется сохранить в файле
Returns: 
  - нет
Файл с именем из <file> и расширением .txt (если передано другое расширение в имени, оно будет заменено на .txt), 
с текстом из <text> сохраняется по пути из <path> с перезаписью файла, если он существовал ранее.
*/
 def saveTextFile (path: String, file: String, text: String): Unit = {
 // Внутрення функция печати текста в файл
 def printToFile(f: java.io.File) (op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() } }
    
  // Создание файла и вызов печати в файл
  printToFile(new File(s"${path}/${file.replaceAll("\\.[^.]*$", "")}.txt")) { p => p.println(text) }
  
  }
}


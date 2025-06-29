package com.simple.sparkparquet

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, lit}

/* Объект - обработчик. 
В данном объекте реальзуется функционал обработки, вызываемый в main - объекте (если требуется какая-то 
дополнительная обработка - удобно добавлять функционал в этом файле)
*/
object Processing {
  /* Обработчик. 
   - формирование сэмпла
   - формирование статистики
  */
  def processor(data: DataFrame, limit_rows: Int, file: (String, Double)): (DataFrame, String) = {
    // Вывод 10 строк
    data.show(10,truncate = false)
    // Статистика
    val statString = s"""
file: ${file._1}
size (kB): ${(file._2/1024).toLong}
count rows: ${data.count}
${data.schema.treeString}
"""
    // Возвращается DataFrame, с заданным количеством строк в сэмпле
    val result = if (limit_rows>0) data.limit(limit_rows) else data
    (result, statString)
  }

}

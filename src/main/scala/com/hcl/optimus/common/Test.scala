package com.hcl.optimus.common

import scala.collection.mutable.ArrayBuffer

object Test {
  def getFolderNameFromPattern(pattern: String) : String = {
    val folderName = pattern match {
      case "" => ""
      case _ => {
        import java.text.SimpleDateFormat
        val simpleDateFormat = new SimpleDateFormat(pattern)
        val folder = simpleDateFormat.format(new java.util.Date()).replaceAll("[ ]", "-").replaceAll("[:]", "-")
        println(s"folder from Pattern $folder")
        java.io.File.separator + folder
      }
    }
    folderName
  }
  def main(args: Array[String]): Unit = {
    val pattern : String = "YYYY-MM-DD HH:mm"
    /*import java.text.SimpleDateFormat
    val simpleDateFormat = new SimpleDateFormat(pattern)
    val folder = simpleDateFormat.format(new java.util.Date()).replaceAll("[ ]", "-").replaceAll("[:]", "-")
    println(folder)*/

    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
    println(capitals.get("XXX").getOrElse(""))
    println(getFolderNameFromPattern(pattern))
    /*val envConfFile = new File("environment.conf")
    val _environmentConfig = ConfigFactory.parseFile(envConfFile).getConfig("conf")
    val environmentConfig = ConfigFactory.load(_environmentConfig)
    
    val classArrayString = environmentConfig.getString("env_config.kryo.registrationRequiredClasses")
    println(classArrayString)
    val classArray = classArrayString.split("\\|").map { x => Class.forName(x) }
    println(classArray)
    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(formatter.parse("abc"))
    var strArray = Array("2016-05-27 13:28:47.000000","2016-05-27 09:18:43.000000","2016-05-27 09:18:43.000000","2016-05-27 09:18:44.000000","2016-05-27 13:27:27.000000","2016-05-28 01:10:00.000000","2016-05-28 01:10:00.000000","2016-05-27 09:18:46.000000","2016-05-28 01:10:00.000000")
    for(date <- strArray) {
      println(new java.sql.Timestamp(formatter.parse(date.toString()).getTime() - 1000))
    }
    
    //println((scala.Tuple2[]).class.getName)
   val x = Array(new java.lang.String)
    val name = x.getClass.getName
    println(name) 
    Class.forName("[Lscala.Byte$;")
     println("Hi")
   Array(Class.forName("scala.Tuple2"))
    println("Neru")*/

    /*val inputWorkbook = new File("SampleS2TM.xlsx");
    type ColIndex = Int 
    type RowIndex = Int

    sealed trait Cell
    case class TextCell(text: String) extends Cell
    case class NumericCell(value: BigDecimal) extends Cell
    // maybe later if needed:
    // case class FunctionCell(function: SomeExcelExpressionType) extends Cell

    type ExcelData = Map[(ColIndex, RowIndex), Cell]
    
    val wb = WorkbookFactory.create(new ByteArrayInputStream(file.get()))
    val sheet = wb.getSheetAt(0)
    
    def getCellString(cell: Cell) = {
      cell.getCellType() match {
        case Cell.CELL_TYPE_NUMERIC => 
          (new DataFormatter()).formatCellValue(cell)
        case Cell.CELL_TYPE_STRING => 
          cell.getStringCellValue()
        case Cell.CELL_TYPE_FORMULA =>
          val evaluator = wb.getCreationHelper().createFormulaEvaluator()
          (new DataFormatter()).formatCellValue(cell, evaluator)
        case _ => " "
      }
    }
    val text = sheet.rowIterator.map(row => {
      row.cellIterator.map(getCellString).mkString("\t")
    }).mkString("\n")*/
       
      
    
    
    
    
    
   // val bufferedSource = scala.io.Source.fromFile("Finance.csv")
    /*val lines = bufferedSource.getLines
    val numOfRows = lines.length
    val numberOfCols = lines.next().split(",").length
    val rows = Array.ofDim[String](numOfRows, numberOfCols)
    for ((line, rowIndex) <- lines.zipWithIndex) {
      rows(rowIndex) = line.split(",").map(_.trim)
    }
    */
    
    /*val rows = ArrayBuffer[Array[String]]()
    for (line <- bufferedSource.getLines.drop(1)) {
      rows += line.split(",").map(_.trim)
    }
    bufferedSource.close
    
    for ((row, rowIndex) <- rows.zipWithIndex) {
      for((column, columnIndex) <- row.zipWithIndex) {
        print(s"(${rowIndex + 1} : ${columnIndex + 1}) = ${column}\t")
      }
      println
    }*/
  }
}
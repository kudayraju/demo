package com.hcl.optimus.common.utilities

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import scala.collection.mutable.{ListBuffer, Map}


trait GetSQLTypes[T, R] extends (T => R)

object GetSQLTypes extends GetSQLTypes[String, DataType] {
  override def apply(sqlType: String): DataType = sqlType match {
    case x if "string" equalsIgnoreCase x => org.apache.spark.sql.types.StringType
    case x if "integer" equalsIgnoreCase x => org.apache.spark.sql.types.IntegerType
    case x if "double" equalsIgnoreCase x => org.apache.spark.sql.types.DoubleType
    case x if "long" equalsIgnoreCase x => org.apache.spark.sql.types.LongType
    case x if "float" equalsIgnoreCase x => org.apache.spark.sql.types.FloatType
    case _ => null
  }
}

object SchemaUtils {

  def constructStructField(colDetails: String): StructField = {

    val colDetArray: Array[String] = colDetails.split('|').map(_.trim)

    colDetArray(1) match {
      case "INTEGER" => StructField(colDetArray(0), IntegerType, true)
      case "DOUBLE" => StructField(colDetArray(0), DoubleType, true)
      case "LONG" => StructField(colDetArray(0), LongType, true)
      case "FLOAT" => StructField(colDetArray(0), FloatType, true)
      case "BYTE" => StructField(colDetArray(0), ByteType, true)
      case "TIMESTAMP" => StructField(colDetArray(0), TimestampType, true)
      case "BOOLEAN" => StructField(colDetArray(0), BooleanType, true)
      case "STRING" => StructField(colDetArray(0), StringType, true)
      //case "DECIMAL" => StructField(colDetArray(0), DecimalType(1,2), true)
      case _ => StructField(colDetArray(0), StringType, true)
    }
  }


  def constructSchema(numberOfColumns: Int, schemaMap: Predef.Map[String, String]): StructType = {
    val fullFieldsMap = Map[Int, StructField]()
    for(i <- 0 to numberOfColumns - 1) {
      fullFieldsMap.put(i.toInt, StructField("COLUMN_" + i, StringType, true) )
    }

    schemaMap.foreach { col =>
      val columnIndex = col._1.toInt
      fullFieldsMap.remove(columnIndex)
      fullFieldsMap.put(columnIndex, constructStructField(col._2.toString))
    }

    val fieldsListBuffer = new ListBuffer[StructField]
    for (i <- 0 to fullFieldsMap.size - 1) {
      fieldsListBuffer += fullFieldsMap.get(i).get
    }
    StructType(fieldsListBuffer.toList)
  }

  def constructSchema(schemaMap: Predef.Map[String, String]): StructType = {
    val fieldsMap = Map[Int, StructField]()
    schemaMap.foreach { col =>
      fieldsMap.put(col._1.toInt, constructStructField(col._2.toString))
    }

    val fieldsListBuffer = new ListBuffer[StructField]
    for (i <- 0 to fieldsMap.size - 1) {
      fieldsListBuffer += fieldsMap.get(i).get
    }
    StructType(fieldsListBuffer.toList)
  }

  def constructAdditionalColumn(additionalColumnMap: Predef.Map[String, String]): Map[String, DataType] = {

    val addedColumnNames = Map[String, DataType]()

    additionalColumnMap.foreach { col =>
      addedColumnNames.put(col._2.toString, GetSQLTypes("String"))
    }
    addedColumnNames
  }

  def getSchemaForTable(tableName: String, configStructuMap: Predef.Map[String, Any]) = {
    val schemaMap: Predef.Map[String, StructType] = getSchemaFromStructureMap(configStructuMap)
    schemaMap.get(tableName).getOrElse(null)
  }

  def getSchemaForTarget(configStructuMap: Predef.Map[String, Any]) = {
    val schemaMap: Predef.Map[String, StructType] = getSchemaFromTablesStructure(configStructuMap)
    if (schemaMap.size == 1) {
      schemaMap.head._2
    } else
      null
  }

  def getSchemaFromStructureMap(configStructuMap: Predef.Map[String, Any]) = {
    val schemaMap = collection.mutable.Map.empty[String, StructType]
    var finalSchema: StructType = null
    val schema: Predef.Map[String, String] = configStructuMap.get("schema").get.asInstanceOf[Predef.Map[String, String]]
    val tableName: String = configStructuMap.get("temp-table-name").get.toString
    val numberOfColumns = configStructuMap.get("number-of-columns").getOrElse("0").toString.toInt
    if(numberOfColumns > 0)
      finalSchema = SchemaUtils.constructSchema(numberOfColumns, schema)
    else
      finalSchema = SchemaUtils.constructSchema(schema)
    schemaMap += (tableName -> finalSchema)

    schemaMap.toMap
  }

  def getSchemaFromTablesStructure(configStructuMap: Predef.Map[String, Any]) = {
    val schemaMap = collection.mutable.Map.empty[String, StructType]
    val tablesListMapWrapper = configStructuMap.get("tables")
    val tablesList = tablesListMapWrapper.get.asInstanceOf[List[Predef.Map[String, Any]]]

    tablesList.foreach { tableMap =>
      var finalSchema: StructType = null
      val schema: Predef.Map[String, String] = tableMap.get("schema").get.asInstanceOf[Predef.Map[String, String]]
      val tableName: String = tableMap.get("temp-table-name").get.toString
      val numberOfColumns = configStructuMap.get("number-of-columns").getOrElse("0").toString.toInt
      if(numberOfColumns > 0)
        finalSchema = SchemaUtils.constructSchema(numberOfColumns, schema)
      else
        finalSchema = SchemaUtils.constructSchema(schema)
      schemaMap += (tableName -> finalSchema)
    }
    schemaMap.toMap
  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName).alias(colName))
      }
    })
  }
}
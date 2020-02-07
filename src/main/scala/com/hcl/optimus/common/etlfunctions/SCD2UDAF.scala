package com.hcl.optimus.common.etlfunctions

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
/**
 * Created by Sanjib Mondal on 19/09/2016.
 * Computes SCD2 Record Position
 */

class SCD2UDAF[T](structType: StructType, t_effectiveStartDate : String, t_effectiveEndDate : String, t_statusFlag : String) extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  //def inputSchema: StructType = new StructType().add("inputSchema", colType)
  def inputSchema: StructType = structType

  // Intermediate Schema
  def bufferSchema: StructType = new StructType().add("buff", ArrayType(structType))

  // Returned Data Type .
  def dataType: DataType = ArrayType(structType)

  // Self-explaining
  def deterministic = true
  //var mutableBuffer : MutableAggregationBuffer = null

  // zero value. This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, new scala.collection.mutable.ArrayBuffer[Row])
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val list = buffer.getSeq[Row](0)
    buffer.update(0, list :+ input)
  }

  // Merge two partial aggregates. Similar to combOp in aggregate
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1.update(0, buffer1.getSeq[Row](0) ++ buffer2.getSeq[Row](0))
  }

  // Called on exit to get return value
  def evaluate(buffer: Row) = {
    val rowSeq = buffer.getSeq[Row](0)
    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val data = rowSeq.toArray
    val updateRowList = data.sortWith((row1, row2) => (row1.getTimestamp(row1.fieldIndex(t_effectiveStartDate)).after(row2.getTimestamp(row2.fieldIndex(t_effectiveStartDate)))))
    val unionColumn = structType.fields.map { _.name }
    var preMapValues = scala.collection.mutable.Map[String, Any]()
      val finalRowList = updateRowList.map { row =>
        val mapValues = collection.mutable.Map[String, Any]() ++ row.getValuesMap(unionColumn)
        if (preMapValues.isEmpty) {
          mapValues(t_effectiveEndDate) = new java.sql.Timestamp(formatter.parse("9999-12-31 00:00:00").getTime())
          mapValues(t_statusFlag) = "Y"
        } else {
          mapValues(t_effectiveEndDate) = {
            preMapValues.get(t_effectiveStartDate) match {
              case Some(date) => new java.sql.Timestamp(formatter.parse(date.toString().trim()).getTime() - 1000)
              case None       => null
            }
          }
          mapValues(t_statusFlag) = "N"
        }
        preMapValues = mapValues
        val emptyList: List[Any] = Nil
        val updatedRecord = (unionColumn zip structType).foldLeft(emptyList)((list, columnAndSchema) => {
          val value = mapValues.get(columnAndSchema._1) match {
            case Some(data) => if(data != null) data.toString()
                                else null
            case None       => null
          }
          val dataType = columnAndSchema._2.dataType
          var columnData : Any = ""
          if(value != null && !value.equalsIgnoreCase("null") && !value.isEmpty()) {
            columnData = dataType match {
            case org.apache.spark.sql.types.IntegerType   => value.toInt
            case org.apache.spark.sql.types.DoubleType    => value.toDouble
            case org.apache.spark.sql.types.LongType      => value.toLong
            case org.apache.spark.sql.types.FloatType     => value.toFloat
            case org.apache.spark.sql.types.ByteType      => value.toByte
            case org.apache.spark.sql.types.StringType    => value
            case org.apache.spark.sql.types.TimestampType => new java.sql.Timestamp(formatter.parse(value.trim()).getTime())
            case default                                  => value
          }
        }
          list :+ columnData
        })
        Row.fromSeq(updatedRecord.toSeq)
      }
    finalRowList
  }
}

/*class SCD2UDAF[T](colType : DataType) extends UserDefinedAggregateFunction {
  
    // Input Data Type Schema
    def inputSchema: StructType = new StructType().add("inputSchema", colType)
  
    // Intermediate Schema
    def bufferSchema : StructType = new StructType().add("bufferSchema", ArrayType(colType))
    
    // Returned Data Type .
    def dataType: DataType = ArrayType(colType)
 
    // Self-explaining
    def deterministic = true
    //var mutableBuffer : MutableAggregationBuffer = null
   
    // zero value. This function is called whenever key changes
    def initialize(buffer: MutableAggregationBuffer) = {
      buffer.update(0, new scala.collection.mutable.ArrayBuffer[T])
    }
    
    // Iterate over each entry of a group
    def update( buffer: MutableAggregationBuffer, input: Row ) = {
      val list = buffer.getSeq[T](0)
      if(!input.isNullAt( 0 ) ) {
        val sales = input.getAs[T](0)
        buffer.update( 0, list:+sales )
      }
    }

    
    // Merge two partial aggregates. Similar to combOp in aggregate
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
      buffer1.update(0, buffer1.getSeq[T](0) ++ buffer2.getSeq[T](0) )
    }
    
    // Called on exit to get return value
    def evaluate(buffer: Row) = {
      buffer.getSeq[T](0)
    }
}*/

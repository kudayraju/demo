package com.hcl.optimus.common.etlfunctions

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.hcl.optimus.common.utilities.md5checksum
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class SCD2UDAFV2[T](structType: StructType, t_effectiveStartDate : String, t_effectiveEndDate : String, t_statusFlag : String, t_MD5Column: String) extends UserDefinedAggregateFunction {

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



  def evaluate(buffer: Row): Array[Row] = {
    val rowSeq = buffer.getSeq[Row](0)
    val formatter: SimpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val data = rowSeq.toArray
    val groupList = new ListBuffer[mutable.Map[String,Any]]()
    val unionColumn: Array[String] = structType.fields.map { _.name }
    var prevDate : String = ""

    val sortedRows = data.sortWith((row1, row2 )=>{
      val row1TimeStamp: Timestamp = new java.sql.Timestamp(formatter.parse(row1.getAs[String](t_effectiveStartDate)).getTime())
      val row2TimeStamp: Timestamp = new java.sql.Timestamp(formatter.parse(row2.getAs[String](t_effectiveStartDate)).getTime())
      val returnVal = row1TimeStamp.after(row2TimeStamp)
      returnVal
    })

    var preMapValues = scala.collection.mutable.Map[String, Any]()
    sortedRows.map( row =>{
      val currMapValues = collection.mutable.Map[String, Any]() ++  row.getValuesMap(unionColumn)
      if( preMapValues.isEmpty ) {
        preMapValues = currMapValues
      }else{
        if( getMD5(currMapValues).equals( getMD5(preMapValues) ) ){
          println("############## SCD2UDAFV2 when MD5 match preMapValues is not Empty, preMapValues :: "+preMapValues+", getMD5(preMapValues) :: "+getMD5(preMapValues)+ ", currMapValues :: "+currMapValues+", getMD5(currMapValues) :: "+getMD5(currMapValues))
          preMapValues = currMapValues
        }else {
          //Added this piece of code to ignore record with same acct_id and effective start date
          import util.control.Breaks._
          breakable {
            if (preMapValues.getOrElse("acct_id", "") == currMapValues.getOrElse("acct_id", "")
              && preMapValues.getOrElse("eff_start_date", "") == currMapValues.getOrElse("eff_start_date", "")) {
              break
            }

            if (groupList.isEmpty) {
              setCurrIndAndEffEndDate(preMapValues, "Y", formatter.parse("9999-12-31 00:00:00"), 0)
              groupList.append(preMapValues)
              prevDate = preMapValues(t_effectiveStartDate).toString
              preMapValues = currMapValues
            } else {
              setCurrIndAndEffEndDate(preMapValues, "N", formatter.parse(prevDate.trim), 1000)
              groupList.append(preMapValues)
              prevDate = preMapValues(t_effectiveStartDate).toString
              preMapValues = currMapValues
            }
         }
        }
      }
    })

    if( !preMapValues.isEmpty ){
      if( groupList.isEmpty) {
        setCurrIndAndEffEndDate(preMapValues, "Y", formatter.parse("9999-12-31 00:00:00"), 0 )
      }else{
        println("############## SCD2UDAFV2 loop ended, groupList is not Empty, preMapValues :: "+preMapValues)
        setCurrIndAndEffEndDate(preMapValues, "N", formatter.parse(prevDate.trim), 1000 )
      }
      prevDate = preMapValues(t_effectiveStartDate).toString
      groupList.append(preMapValues)
    }
    groupList.toArray.map( modifyDataType(unionColumn, _, formatter) )
  }

  def modifyDataType( unionColumn: Array[String], mapValues : mutable.Map[String, Any], formatter: SimpleDateFormat  ): Row ={
    val dataTypeModifiedRecord = (unionColumn zip structType).foldLeft(List.empty[Any])((list, columnAndSchema) => {
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
    Row.fromSeq(dataTypeModifiedRecord.toSeq)
  }

  def getMD5( inputMap : mutable.Map[String, Any] ): String ={
    val md5ColumnValues = t_MD5Column.split(",").map(attr => (attr, inputMap.get(attr.toLowerCase).get.toString )).toList
    println("################ md5ColumnValues :: "+md5ColumnValues)
    md5checksum.getMD5CheckSum( t_MD5Column.split(",").map(attr => inputMap.get(attr.toLowerCase).get.toString ):_* )
  }

  def setCurrIndAndEffEndDate(inputMap: mutable.Map[String, Any], currInd: String, date: Date, reduce : Int) = {
    inputMap(t_statusFlag) = currInd
    inputMap(t_effectiveEndDate) = new java.sql.Timestamp(date.getTime - reduce)
  }
}

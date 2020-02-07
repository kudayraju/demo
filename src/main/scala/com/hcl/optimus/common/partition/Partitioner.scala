package com.hcl.optimus.common.partition

import com.hcl.optimus.common.utilities.Arrow
import com.hcl.optimus.context.Context
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

object PartitionUtils extends Arrow {

  def getKeyForColumnValue(data: Row)(columnIndexValue: Map[String, Any]): String = {
    val concatKey = new StringBuilder
    columnIndexValue.map(keyVal => {
      val key: String = keyVal._1
      val kValue: String = keyVal._2.asInstanceOf[String]
      val dValue: String = key ~> data.getAs[String]
      if (kValue ~> StringUtils.isNotEmpty && StringUtils.containsIgnoreCase(dValue, kValue))
        kValue ~> concatKey.append
      else
        dValue ~> concatKey.append
    })
    concatKey.toString
  }

  def getKeyFromColumnLength(data: Row)(columnIndexValue: Map[String, Any]): String = {
    val concatKey = new StringBuilder
    columnIndexValue.map(keyVal => {
      val key: String = keyVal._1
      val kValue: String = keyVal._2.asInstanceOf[String]
      val indexes: List[String] = kValue.split(",").toList
      val startIndex = Integer.parseInt(indexes(0))
      val length = Integer.parseInt(indexes(1))
      val rawColumnValue = key ~> data.getAs[String]
      val compareValue = StringUtils.substring(rawColumnValue, startIndex, length)

      if (compareValue ~> StringUtils.isNotEmpty && StringUtils.containsIgnoreCase(rawColumnValue, compareValue)) {
        compareValue ~> concatKey.append
      } else {
        rawColumnValue ~> concatKey.append
      }
    })
    concatKey.toString
  }
}

trait ConvertDataToKeyAndRowForValue[T, P, R] extends ((T, P) => R)

object ConvertDataToKeyAndRowForValue {

  implicit object ConvertDataToKeyAndRowForValueObj extends ConvertDataToKeyAndRowForValue[DataFrame, Map[String, Any], RDD[(String, List[Row])]] {
    override def apply(inputDF: DataFrame, columnIndexValueTuple: Map[String, Any]): RDD[(String, List[Row])] = {
      inputDF.rdd.mapPartitions(partition => {
        partition.map(rowValue => {
          val keyListValue = List.empty[Row]
          val rowKey = PartitionUtils.getKeyForColumnValue(rowValue)(columnIndexValueTuple)
          rowKey -> (keyListValue :+ rowValue)
        }).toList.iterator
      })
    }
  }

}

trait ConvertKeyIndexDataToKeyAndRowForValue[T, P, R] extends ((T, P) => R)

object ConvertKeyIndexDataToKeyAndRowForValue {

  implicit object ConvertKeyIndexDataToKeyAndRowForValueObj extends ConvertKeyIndexDataToKeyAndRowForValue[DataFrame, Map[String, Any], RDD[(String, List[Row])]] {
    override def apply(inputDF: DataFrame, columnIndexValueTuple: Map[String, Any]): RDD[(String, List[Row])] = {
      inputDF.rdd.mapPartitions(partition => {
        partition.map(rowValue => {
          val keyListValue = List.empty[Row]
          val rowKey = PartitionUtils.getKeyFromColumnLength(rowValue)(columnIndexValueTuple)
          rowKey -> (keyListValue :+ rowValue)
        }).toList.iterator
      })
    }
  }

}

trait CreateDataFrameFromRDD[T, P, R] extends ((T, P) => R)

object CreateDataFrameFromRDD {

  implicit object CreateDataFrameFromRDDObj extends CreateDataFrameFromRDD[RDD[(String, List[Row])], StructType, DataFrame] {
    override def apply(reducedKeyData: RDD[(String, List[Row])], originalInputSchema: StructType): DataFrame = {
      val rddOfRows: RDD[Row] = reducedKeyData.mapPartitions { rowIterator =>
        val rowList: List[Row] = rowIterator.flatMap(fx => {
          fx._2
        }).toList
        rowList.iterator
      }
      Context.getSqlContext().createDataFrame(rddOfRows, originalInputSchema)
    }
  }

}

trait ColumnValuePartitioner[T, R] extends ((T, R) => T)

object ColumnValuePartitioner {

  val KeyAndRowConvert = implicitly[ConvertDataToKeyAndRowForValue[DataFrame, Map[String, Any], RDD[(String, List[Row])]]]
  val rdd2df = implicitly[CreateDataFrameFromRDD[RDD[(String, List[Row])], StructType, DataFrame]]

  implicit object ColumnValuePartitionerObj extends ColumnValuePartitioner[DataFrame, Map[String, Any]] {
    override def apply(inputDF: DataFrame, columnValueMappingTuple: Map[String, Any]): DataFrame = {
      val reducedKeyData: RDD[(String, List[Row])] = KeyAndRowConvert(inputDF, columnValueMappingTuple).reduceByKey(_ ++ _)
      rdd2df(reducedKeyData, inputDF.schema)
    }
  }

}

trait ColumnIndexLengthPartitioner[T, R] extends ((T, R) => T) with Arrow

object ColumnIndexLengthPartitioner {

  val KeyAndRowConvert = implicitly[ConvertKeyIndexDataToKeyAndRowForValue[DataFrame, Map[String, Any], RDD[(String, List[Row])]]]
  val rdd2df = implicitly[CreateDataFrameFromRDD[RDD[(String, List[Row])], StructType, DataFrame]]

  implicit object ColumnIndexLengthPartitionerObj extends ColumnIndexLengthPartitioner[DataFrame, Map[String, Any]] {
    override def apply(inputDF: DataFrame, columnIndexLengthMapping: Map[String, Any]): DataFrame = {
      val reducedKeyData: RDD[(String, List[Row])] = KeyAndRowConvert(inputDF, columnIndexLengthMapping).reduceByKey(_ ++ _)
      rdd2df(reducedKeyData, inputDF.schema)
    }
  }

}
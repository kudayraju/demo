package com.hcl.optimus.common.utilities

import com.hcl.optimus.context.Context
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

@SerialVersionUID(100L)
sealed case class _Reader() extends Serializable {
//sealed case class _Reader() {
  
    /*def readHDFS(hdfsLocation : String, separator : String, columns : Seq[String]) : DataFrame = {
        val fileRDD = context.sc.textFile(hdfsLocation)
        val rowRDD = fileRDD.map(line => jobUtility.getRow(line.split(separator), columns))
        val schema = jobUtility.getSchema(columns)
        context.hiveContext.createDataFrame(rowRDD, schema).toDF()
    }*/

    def readHive(statement : String) : DataFrame = {
        Context.getSparkSession().sql(statement)
    }
  
    private def createSchemaString(fileRDD : RDD[String], separator : String) : String = {
        val columnLength = fileRDD.first.split(separator).length
        val x = 0 to (columnLength - 1)
        x.map("col"+_).mkString(" ")
    }
}

object reader extends _Reader()
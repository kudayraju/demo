package com.hcl.optimus.common.utilities

import com.hcl.optimus.context.Context
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

@SerialVersionUID(100L)
sealed case class _JobUtility() extends Serializable with Logging {
  //sealed case class _JobUtility() {
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
  def loadFromHiveTable(hiveTableName: String, serachColumns: String = "*") = {
    val df = reader.readHive("select " + serachColumns + " from " + hiveTableName)
    df
  }

  def saveDFtoTargetTable(finalDF: DataFrame, targetTableName: String, saveMode: SaveMode) = {
    finalDF.write.mode(saveMode).saveAsTable(targetTableName)
    //finalDF.saveAsTable(targetTableName, saveMode)
  }

  def saveDFtoTargetLocation(finalDF: DataFrame, targetLocation: String, saveMode: SaveMode) = {
    //finalDF.save(targetLocation, saveMode)
    finalDF.write.mode(saveMode).save(targetLocation)
  }

  def isEmpty(dataFrame: DataFrame): Boolean = {
    val length = dataFrame.take(1).length
    if (length > 0) {
      logDebug("Record present in DF ")
      false
    } else {
      true
    }
  }


  def createDFAsLikeTarget(rawDF: DataFrame, targetSchema: StructType): DataFrame = {
    Context.getSparkSession().createDataFrame(rawDF.rdd, targetSchema)
  }
  /*def getJoinColumnExpression(targetTableKeyColumnNameSequences : Seq[String]) : org.apache.spark.sql.Column = { 
        val headColumnName = targetTableKeyColumnNameSequences.head
        val startJoinColumn = new org.apache.spark.sql.Column(headColumnName) === new org.apache.spark.sql.Column(headColumnName+"TEMP")
  			return targetTableKeyColumnNameSequences.foldLeft(startJoinColumn)((column, targetTableKeyColumnName) => column && new org.apache.spark.sql.Column(targetTableKeyColumnName) === new org.apache.spark.sql.Column(targetTableKeyColumnName+"TEMP"))
	  }
    
    def getJoinColumnExpressionFromKeyColumns(columns : Seq[String]) : org.apache.spark.sql.Column = { 
        val headColumnName = columns.head
        val startJoinColumn = new org.apache.spark.sql.Column(headColumnName) === new org.apache.spark.sql.Column(headColumnName+"TEMP")
  			return columns.foldLeft(startJoinColumn)((joinColumn, column) => joinColumn && new org.apache.spark.sql.Column(column) === new org.apache.spark.sql.Column(column+"TEMP"))
	  }
	  
	  def saveDFtoTargetLocation(finalDF : DataFrame, targetLocation : String) = {
        	val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
       		try { 
              	hdfs.delete(new org.apache.hadoop.fs.Path(targetLocation), true) 
              	finalDF.save(targetLocation + "/default/", SaveMode.Overwrite) 
	            	finalDF.write.format("com.databricks.spark.csv").option("header", "false").mode(SaveMode.Overwrite).save(targetLocation + "/csv/out.csv")
	            	finalDF.write.mode(SaveMode.Append).parquet(targetLocation)
	            	import org.apache.spark.sql.functions.{concat_ws, col}
              	import org.apache.spark.sql.Row

              	val expr = concat_ws(",", finalDF.columns.map(col): _*)
              	finalDF.select(expr).map(_.getString(0)).saveAsTextFile (targetLocation)
              	//finalDF.write.mode(SaveMode.Append).parquet(targetLocation)
	            	finalDF.save(targetLocation, SaveMode.Overwrite)
         //} catch { case _ : Throwable => { } }
	  }   
    def getJoinColumnExpressionFromKeyColumns(s_columns : Seq[String], t_columns : Seq[String]) : org.apache.spark.sql.Column = { 
        val startJoinColumn : org.apache.spark.sql.Column = null
        return (s_columns zip t_columns).foldLeft(startJoinColumn)((joinColumn, columns) => if(joinColumn != null) (joinColumn && new org.apache.spark.sql.Column(columns._1 + "SRC") === new org.apache.spark.sql.Column(columns._2 + "TGT")) else (new org.apache.spark.sql.Column(columns._1) === new org.apache.spark.sql.Column(columns._2 + "TGT")))
        /*val s_headColumnName = s_columns.head
        val t_headColumnName = t_columns.head
        val startJoinColumn = new org.apache.spark.sql.Column(s_headColumnName) === new org.apache.spark.sql.Column(t_headColumnName + "TEMP")
            
        return (s_columns zip t_columns).foldLeft(startJoinColumn)((joinColumn, columns) => joinColumn && new org.apache.spark.sql.Column(columns._1) === new org.apache.spark.sql.Column(columns._2+"TEMP"))*/
	  }*/

}

object jobUtility extends _JobUtility()
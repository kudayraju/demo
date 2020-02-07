package com.hcl.optimus.common.utilities

import java.io._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame


trait StoreValueInFile[T,R] extends (T => R)
object StoreValueInFile extends StoreValueInFile[(String,String), Unit] {
  override def apply(storeParams: (String, String)): Unit = {
    val bufferedWriter: BufferedWriter = storeParams._1.trim match{
      case s if s == null || s == "" => throw new Exception("sourceIncrementalFileLoc should be a edge node or hdfs path, but coming as Null or Empty")
      case s if s.toLowerCase.startsWith("hdfs://") =>
        val subStringHdfs = s.substring("hdfs://".length)
        val hdfs = FileSystem.get(URI.create(subStringHdfs), new Configuration())
        new BufferedWriter(new OutputStreamWriter( hdfs.create(new Path(subStringHdfs))))
      case _ => new BufferedWriter(new FileWriter(new File(storeParams._1.trim)))
    }
    bufferedWriter.write(storeParams._2)
    bufferedWriter.close()
  }
}

object StoreApplicationState {

  implicit class StoreDataBaseSate( dataFrame : DataFrame){
    def updateIncrementalCoulmnValue( sourceFullloadOrIncremental : String, sourceIncrementalColumn : String, sourceIncrementalFileLoc: String)={
      sourceFullloadOrIncremental.trim match{
        case s if "FULLLOAD".equalsIgnoreCase(s) => None
        case s if "INCREMENTAL".equalsIgnoreCase(s) =>
          if(!_JobUtility().isEmpty(dataFrame)){
            dataFrame.registerTempTable("inputDF")
            //val columnValue = dataFrame.sqlContext.sql(s"SELECT max($sourceIncrementalColumn) as maxValue FROM inputDF").first().getAs[Integer]("maxValue").toString
            val columnValue = dataFrame.sqlContext.sql(s"SELECT max($sourceIncrementalColumn) as maxValue FROM inputDF").first().get(0).toString
			StoreValueInFile(sourceIncrementalFileLoc, columnValue)
          }
        case _ => throw new Exception(s"sourceFullloadOrIncremental value should be Fullload or Incremental, not recognising ${sourceFullloadOrIncremental}")
      }
      dataFrame
    }
  }
}

package com.hcl.optimus.common.utilities


import com.google.common.io.ByteStreams
import com.hcl.optimus.common.log.logger.Logging
import com.hcl.optimus.context.Context
import org.apache.commons.codec.digest._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.io.Source


object CommonUtility extends Logging {

  def getFileBytes(Filepath: String): Array[Byte] = {
    var fileByteArr: Array[Byte] = null
    if (StringUtils.isNotBlank(Filepath) && Filepath.startsWith("hdfs://")) {
      val hdfsFilePath = Filepath.substring("hdfs://".length())
      val configuration = new Configuration();
      val path = new Path(hdfsFilePath);

      val fs = FileSystem.get(configuration);
      val inputStream = fs.open(path)
      fileByteArr = ByteStreams.toByteArray(inputStream)
    } else {
      fileByteArr = Source.fromFile(Filepath, "UTF-8").map {
        _.toByte
      }.toArray
    }
    fileByteArr
  }

  def getFilePathTuple(dataFilePath: String): (String, String) = {
    val accountFQDN: String = dataFilePath.split("/")(2)
    val absDirPath: String = dataFilePath.substring(dataFilePath.indexOf(accountFQDN) + accountFQDN.length)
    (accountFQDN, absDirPath)
  }

  def getCurrentDateWithFormat(dateFormat: String): String = {
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter

    DateTimeFormatter.ofPattern(dateFormat).format(LocalDateTime.now)
  }

}

@SerialVersionUID(100L)
object DataFrameOperation extends Serializable with Logging {

  import org.apache.spark.sql.functions._

  private def hashedKey(input: String): String = {
    DigestUtils.sha512Hex(input)
  }

  implicit class _DataFrameOperation(val inputDF: DataFrame) extends Serializable {

    val surrogateKeyUDF: UserDefinedFunction = udf(hashedKey _)

    def surrogateKey(): DataFrame = {

      try {

        val newColumnName = Context.getJobConfigurationParameterForJSONConfig("job_config.TargetTable.name").concat("_sKey")
        val inputKey = Context.getJobConfigurationParameterForJSONConfig("job_config.TargetTable.keyColumns")

        if(inputKey.split(",").toSeq.size > 1)
          throw new Exception("Surrogate Key - can't be generated on multiuple keyColumns [ key : job_config.TargetTable.keyColumns ]")

        inputDF.select(surrogateKeyUDF(col( inputKey )).alias(newColumnName), inputDF.col("*"))

      } catch {
        case exp: Throwable => {
          inputDF
        }
      }
    }

    def flattenDataframe(): DataFrame = {

      val fields = inputDF.schema.fields
      val fieldNames = fields.map(x => x.name)
      val length = fields.length

      for(i <- 0 to fields.length-1){

        val field = fields(i)
        val fieldtype = field.dataType
        val fieldName = field.name

        fieldtype match {

          case arrayType: ArrayType =>
            val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
            val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
            val explodedDf = inputDF.selectExpr(fieldNamesAndExplode:_*)
            return explodedDf.flattenDataframe()

          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
            val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
            val explodedf = inputDF.select(renamedcols:_*)
            return explodedf.flattenDataframe()

          case _ =>

        }
      }
      inputDF
    }



    def addCreateDateColumn(dateValue: String) : DataFrame = {
      inputDF.withColumn("created_date", lit(dateValue))
    }

    def xsltTransformation(xsltPath: String, targetSchema: StructType): DataFrame = {

      val transformedRdd = ("NA" == xsltPath) match {
        case false => {
          val fieldNames = inputDF.columns
          val xsltStr = Context.getSparkContext().textFile(xsltPath).collect().mkString

          import com.hcl.xsltutil.XSLTTransformer
          import inputDF.sparkSession.implicits._
          implicit val encoder = RowEncoder.apply(inputDF.schema)
          val stringRDD = inputDF.mapPartitions { rowIterator => {
            val transformer = XSLTTransformer.getTransformer(xsltStr)
            rowIterator.map(row => XSLTTransformer.transform(toXML(row, fieldNames), transformer))
          }
          }

          val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val rowRDD = stringRDD.map { str =>
            val seqData = str.split("\\|").toSeq
            val emptyList: List[Any] = Nil
            val updatedRecord = (seqData zip targetSchema).foldLeft(emptyList)((list, dataAndSchema) => {
              val value = dataAndSchema._1
              val dataType = dataAndSchema._2.dataType
              var columnData: Any = ""
              if (value != null && !value.equalsIgnoreCase("null") && !value.isEmpty()) {
                columnData = dataType match {
                  case org.apache.spark.sql.types.IntegerType => value.toInt
                  case org.apache.spark.sql.types.DoubleType => value.toDouble
                  case org.apache.spark.sql.types.LongType => value.toLong
                  case org.apache.spark.sql.types.FloatType => value.toFloat
                  case org.apache.spark.sql.types.ByteType => value.toByte
                  case org.apache.spark.sql.types.DecimalType() => scala.math.BigDecimal(value)
                  case org.apache.spark.sql.types.StringType => value
                  case org.apache.spark.sql.types.TimestampType => new java.sql.Timestamp(formatter.parse(value.trim()).getTime())
                  case default => value
                }
              } else
                columnData = null
              list :+ columnData
            })
            Row.fromSeq(updatedRecord.toSeq)
          }
          rowRDD.rdd
        }
        case true => {
          inputDF.rdd
        }
      }

      Context.getSparkSession().createDataFrame(transformedRdd, targetSchema)
    }

    private val toXML: (Row, Seq[String]) => String = (row: Row, fieldNames: Seq[String]) => {
      val mapValues: Map[String, Nothing] = row.getValuesMap(fieldNames)
      var sb = new StringBuilder("<elements>")

      fieldNames.foreach { fieldName =>
        sb.append("<" + fieldName.toLowerCase + ">")
        //sb.append("<" + fieldName + ">")
        val fieldValue = if (mapValues(fieldName) == null) "" else mapValues(fieldName).toString()
        sb.append(fieldValue)
          .append("</" + fieldName.toLowerCase + ">")
          //.append("</" + fieldName + ">")
      }
      sb.append("</elements>")

      sb.toString()
    }
  }

}
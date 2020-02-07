package com.hcl.optimus.context

import java.io.{BufferedReader, FileReader, InputStreamReader}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hcl.optimus.common.utilities.Arrow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext


@SerialVersionUID(100L)
object Context extends Serializable with Logging with Arrow {

  private var sparkContext: SparkContext = null
  private var sqlContext: SQLContext = null
  private var hiveContext: HiveContext = null
  private var sparkSession: SparkSession = null
  private var sparkStreamingContext :StreamingContext = null

  var applicationId: String = ""

  private val MASTER_URL = "spark.master"
  private val DEFAULT_MASTER = "local[2]"
  var environmentConfig = scala.collection.mutable.Map[String, String]()
  var jobConfig = scala.collection.mutable.Map[String, String]()
  var jobMapConfig = scala.collection.mutable.Map[String, Any]()

  private var jobName: String = null

  private def createApplicationId() = {
    // Get the JOB ID
    applicationId = this.jobName//sparkContext.applicationId
  }

  def createContext(jobName: String, jobPropertyPath: String, environemntPropertyPath: String, configsInSubmit: scala.collection.mutable.Map[String, String]) = {
    loadContext(jobName, jobPropertyPath, environemntPropertyPath, configsInSubmit)
  }

  private val loadContext: (String, String, String, scala.collection.mutable.Map[String, String]) => Unit = (jobName, jobPropertyPath, environemntPropertyPath, configsInSubmit) => {
    this.jobName = jobName

    // Load Environemnt
    loadEnvironemntParameters(environemntPropertyPath)

    val classArrayString = getEnvConfigurationParameter("env_config.kryo.registrationRequiredClasses")
    val classArray = classArrayString.split("\\|").map { x => Class.forName(x) }

    val sparkConf: SparkConf = new SparkConf()
    sparkConf.registerKryoClasses(classArray)
    sparkConf.registerKryoClasses(Array(classOf[Array[scala.Tuple2[_, _]]], Class.forName("[[B"), classOf[Array[scala.Int]], classOf[Array[scala.Boolean]], classOf[Array[scala.Char]]))
    sparkConf.set("spark.scheduler.mode","FAIR")
    sparkConf.set("spark.locality.wait","1s")

    val master: String = sparkConf.get(MASTER_URL, DEFAULT_MASTER)
    SparkSession.clearActiveSession()
    val sparkSession = SparkSession.builder().config(sparkConf)
      //.master(master)
      .appName(jobName)
      .enableHiveSupport()
      .getOrCreate()

    this.sparkSession = sparkSession
    sqlContext = sparkSession.sqlContext
    sparkContext = sparkSession.sparkContext

    sparkStreamingContext = new StreamingContext(sparkContext, Seconds(10))

    // set application id for job name
    createApplicationId

    loadConfigurationParameter(jobPropertyPath, configsInSubmit)

    val logLevel = getJobConfigurationParameterForJSONConfig("job_config.logLevel")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.toLevel(logLevel, Level.FATAL))
  }

  private def loadProperty(propertyPath: String): BufferedReader = {
    propertyPath toLowerCase match {
      case s if s.startsWith("hdfs://") => {
        val hdfs = FileSystem.get(new Configuration())
        new BufferedReader(new InputStreamReader(hdfs.open(new Path(propertyPath.substring("hdfs://".length)))))
      }
      case s if s.startsWith("dbfs:") => {
        import scala.io.Source
        new BufferedReader(Source.fromFile(propertyPath).bufferedReader())
      }
      case _ => {
        new BufferedReader(new FileReader(propertyPath))
      }
    }
  }

  private def setupFileSystemWithPath(filePathWithPlatform: (String, String)): BufferedReader = {
    val filePath = filePathWithPlatform._2
    filePathWithPlatform._1 toLowerCase match {
      case "hdfs" => {
        val hdfs = FileSystem.get(new Configuration())
        new BufferedReader(new InputStreamReader(hdfs.open(new Path(filePath))))
      }
      case "dbfs" => {
        import scala.io.Source
        new BufferedReader(Source.fromFile(filePath).bufferedReader())
      }
      case _ => {
        new BufferedReader(new FileReader(filePath))
      }
    }
  }

  private def getStrToAnyConfigurationMap(propBufferReader: BufferedReader): Predef.Map[String, Any] = {
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val tempPropertyRead: Map[String, Any] = mapper.readValue(propBufferReader, classOf[Predef.Map[String, Any]])

    var finalMap = Predef.Map[String, Any]()
    tempPropertyRead.map( tuple => {
      val key = applicationId + "_" + tuple._1
      // Add JOB id with every key
      finalMap += (key -> tuple._2)
    })
    finalMap
  }

  def getConfigurationMap(parsedConfig: Config): Predef.Map[String, String] = {
    import scala.collection.JavaConverters._
    parsedConfig.entrySet().asScala.foldLeft(Predef.Map.empty[String, String]) {
      case (map, entrySet) =>
        map.updated(entrySet.getKey, entrySet.getValue.unwrapped().toString)
    }
  }

  private def loadEnvironemntParameters(environemntPropertyPath: String ) : Unit = {
    environmentConfig = environmentConfig ++ getConfigurationMap(ConfigFactory.parseReader(loadProperty(environemntPropertyPath)))
  }

  private def loadExternalParameterConfiguration(configsInSubmit: scala.collection.mutable.Map[String, String]):  scala.collection.mutable.Map[String, String] = {
    var finalMap = scala.collection.mutable.Map[String, String]()
    configsInSubmit.map( tuple => {
      val key = applicationId + "_" + tuple._1
      // Add JOB id with every key
      finalMap += (key -> tuple._2)
    })
    finalMap
  }

  private def loadConfigurationParameter(jobPropertyPath: String, configsInSubmit: scala.collection.mutable.Map[String, String]): Unit = {

    jobMapConfig = jobMapConfig ++ getStrToAnyConfigurationMap(loadProperty(jobPropertyPath))

    // Add external key-value in Map
    jobMapConfig = jobMapConfig ++ loadExternalParameterConfiguration(configsInSubmit)

    /**
      * Identify file syatem and as well as file path from file system
      *
      * Example:
      * --------
      * "fileSystem:dbfs", [value can be hdfs/dbfs]
      * "filePath:/dbfs/FileStore/sketch/pipeline_loadTransformScd2SaveADLS_OptimusV2/authentication.conf"
      */
    val fileSystem: String = configsInSubmit.getOrElse("fileSystem", "")
    val filePathWithPlatform: (String, String) = fileSystem match {
      case "dbfs" | "hdfs" => (fileSystem, configsInSubmit.getOrElse("filePath", ""))
      case _ => ("", configsInSubmit.getOrElse("filePath", ""))
    }

    // Setup file path into map
    if(!filePathWithPlatform._2.equals(""))
      jobMapConfig = jobMapConfig ++ getStrToAnyConfigurationMap(setupFileSystemWithPath(filePathWithPlatform))
  }

  def getEnvConfigurationParameter(key: String): String = {
    environmentConfig.getOrElse(key, "")
  }

  def getJobConfigurationMap(key: String): Predef.Map[String, Any] = {
    // Generate Key with application id
    //jobMapConfig.get(key).get.asInstanceOf[Predef.Map[String, Any]]
    jobMapConfig.get(applicationId + "_" + key).get.asInstanceOf[Predef.Map[String, Any]]
  }

  def getJobConfigurationMapForJSONConfig(key: String): Predef.Map[String, Any] = {
    // New way of split
    //val keys = key.split("[.]")
    val keys = (applicationId + "_" + key).split("[.]")

    val depth = keys.length
    var a = 0;
    var tempMap: scala.collection.mutable.Map[String, Any] = jobMapConfig
    // for loop execution with a range
    for (a <- 1 to depth) {
      tempMap = collection.mutable.Map(tempMap.get(keys(a - 1)).get.asInstanceOf[Predef.Map[String, Any]].toSeq: _*)
    }
    tempMap.toMap
  }

  def getJobConfigurationParameter(key: String): String = {
    jobConfig.get(key).get
  }

  def getJobConfigurationParameterForJSONConfig(key: String): String = {
    // New way of split
    val keys = (applicationId + "_" + key).split("[.]")

    val depth = keys.length
    var a = 0;
    var tempMap: scala.collection.mutable.Map[String, Any] = jobMapConfig

    // for loop execution with a range
    for (a <- 1 to depth - 1) {
      tempMap = collection.mutable.Map(tempMap.get(keys(a - 1)).get.asInstanceOf[Predef.Map[String, Any]].toSeq: _*)
    }
    tempMap.get(keys(depth - 1)).get.asInstanceOf[String]
  }

  def getSparkContext() = {
    sparkContext
  }

  def getSqlContext() = {
    sqlContext
  }

  def getJobName() = {
    jobName
  }

  def getSparkSession() = {
    sparkSession
  }
  def getJobMapConfig :scala.collection.mutable.Map[String, Any] = {
    jobMapConfig
  }

  def getSparkStreamingContext() :StreamingContext = {
    sparkStreamingContext
  }
}


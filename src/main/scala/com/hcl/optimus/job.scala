package com.hcl.optimus

import com.hcl.optimus.context.Context
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

abstract class job extends Serializable with Logging {
  def main(args: Array[String]): Unit = {
    var jobName: String = ""
    try {
      /*if (null != args && args.length != 6) {
        val exceptionMessage = "Proper Usage is: <jobName> <jobPropertyPath> <environemntPropertyPath> <instanceProcessId> <currentTimeStamp> <xsltPath>"
        logError(exceptionMessage, new Exception(this.getClass.getCanonicalName + ": \n" + exceptionMessage))
        System.exit(1)
      }*/
      if (!(args.length >= 3)) {
        val exceptionMessage = "Proper Usage is: <jobName> <jobPropertyPath> <environemntPropertyPath> [key1|value1] [key2|value2]"
        logError(exceptionMessage, new Exception(this.getClass.getCanonicalName + ": \n" + exceptionMessage))
        System.exit(1)
      }
      jobName = args(0)
      val jobPropertyPath: String = args(1)
      val environemntPropertyPath: String = args(2)
      var configsInSubmit = scala.collection.mutable.Map[String, String]()

      for(i <- 3 to args.length-1) {
        val confArr = args(i).split("[:]")
        configsInSubmit.put(confArr(0), confArr(1))
      }
      logInfo("configsInSubmit : " + configsInSubmit)
      
      /*val instanceProcessId: String = args(3)
      val currentTimeStamp: String = args(4)
      val xsltPath : String = args(5)*/
      setup(jobName, jobPropertyPath, environemntPropertyPath, configsInSubmit)
      //perform(instanceProcessId, currentTimeStamp, xsltPath)
      perform()
    } catch {
      case t: Throwable => { //t.printStackTrace() // TODO: handle error
        logError(t.getMessage())
        t.printStackTrace()
      }
    } finally {
      cleanup(jobName)
    }
  }

  def setup(jobName: String, jobPropertyPath: String, environemntPropertyPath : String, configsInSubmit:scala.collection.mutable.Map[String, String]): Unit = {
    val startTime = System.currentTimeMillis()
    Context.createContext(jobName, jobPropertyPath, environemntPropertyPath, configsInSubmit)
    /*val xsltStr = Context.getSparkContext().textFile(xsltPath).collect().mkString
    import com.hcl.xsltutil.XSLTTransformer
    val transformer = XSLTTransformer.getTransformer(xsltStr)
    Context.transformerBroadcasted = Context.getSparkContext().broadcast(transformer)*/
    val endTime = System.currentTimeMillis()
    logDebug("Time Taken : " + (endTime - startTime) / 1000);
  }

  //def perform(instanceProcessId: String, currentTimeStamp: String, xsltPath : String): Unit = {
  def perform(): Unit = {
    val startTime = System.currentTimeMillis()
    /*val targetTableName = Context.getJobConfigurationParameter("job_config.TargetTable.name")
    val targetTableLocation = Context.getJobConfigurationParameter("job_config.TargetTable.location")
    *///execute(instanceProcessId, currentTimeStamp, targetTableName, targetTableLocation, xsltPath)
    execute()
    val endTime = System.currentTimeMillis()
    logDebug("Time Taken : " + (endTime - startTime) / 1000);
  }

  //def execute(instanceProcessId: String, currentTimeStamp: String, targetTable: String, tableLocation: String, xsltPath : String)
  def execute()

  def cleanup(jobName: String): Unit = {
    val startTime = System.currentTimeMillis()
    /*val sparkContext = Context.getSparkContext()
    if (sparkContext != null) {
      sparkContext.stop()
    }*/

    val litsOfAppIDDeleteForJobConfig: List[String] = List.empty[String]
    val litsOfAppIDDeleteForJobMapConfig: List[String] = List.empty[String]

    // Find keys from job config for delete
    Context.jobConfig.map (tuple => {
      if( StringUtils.startsWithIgnoreCase(jobName, tuple._1) ) {
        litsOfAppIDDeleteForJobConfig +: tuple._1
      }
    })

    // Delete from jobConfig
    litsOfAppIDDeleteForJobConfig.map( element => {
      Context.jobConfig.remove(element)
    })

    // Find keys from job mao config
    Context.jobMapConfig.map (tuple => {
      if( StringUtils.startsWithIgnoreCase(jobName, tuple._1) ) {
        litsOfAppIDDeleteForJobMapConfig +: tuple._1
      }
    })

    // Delete from jobMapConfig
    litsOfAppIDDeleteForJobMapConfig.map( element => {
      Context.jobMapConfig.remove(element)
    })

    val endTime = System.currentTimeMillis()
    logDebug("Time Taken : " + (endTime - startTime) / 1000);
  }
}

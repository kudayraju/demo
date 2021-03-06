package com.hcl.optimus.common.log.logger

import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.log4j.PropertyConfigurator
import org.slf4j.impl.StaticLoggerBinder
import java.net.URL
import scala.io.Source
import org.slf4j.impl.Log4jLoggerAdapter
import com.hcl.optimus.common.log.customlevels.CustomLevel
import org.apache.log4j.Priority
import java.security.MessageDigest
import java.util.UUID

trait Logging {
  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Log methods that take only a String
  protected def logInfo( msg: => String  ) {
    if (log.isInfoEnabled) log.info( msg )
  }

  protected def logDebug( msg: => String ) {
    if (log.isDebugEnabled) log.debug( msg )
  }

  protected def logTrace( msg: => String ) {
    if (log.isTraceEnabled) log.trace( msg )
  }

  protected def logWarning( msg: => String ) {
    if (log.isWarnEnabled) log.warn( msg )
  }

  protected def logError( msg: => String ) {
    if (log.isErrorEnabled) log.error( msg )
  }

  
  protected def logCustomLevel( level : String, identifier: => String, msg: => String, addHasing : Boolean = true ) {
    try{
      val field = Class.forName("org.slf4j.impl.Log4jLoggerAdapter").getDeclaredField("logger")
      field.setAccessible(true)
      val log4jLog = field.get(log).asInstanceOf[ org.apache.log4j.Logger ]
      log4jLog.log(CustomLevel.toLevel( level ),  updateLogMessage( msg, identifier, addHasing ) )
    }catch{
      case ex : Exception => ex.printStackTrace //println("")
    }
  }
  
  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo( msg: => String, throwable: Throwable ) {
    if (log.isInfoEnabled) log.info( msg, throwable)
  }

  protected def logDebug( msg: => String, throwable: Throwable ) {
    if (log.isDebugEnabled) log.debug( msg, throwable)
  }

  protected def logTrace( msg: => String, throwable: Throwable ) {
    if (log.isTraceEnabled) log.trace( msg, throwable)
  }

  protected def logWarning( msg: => String, throwable: Throwable ) {
    if (log.isWarnEnabled) log.warn( msg, throwable)
  }

  protected def logError( msg: => String, throwable: Throwable ) {
    if (log.isErrorEnabled) log.error( msg, throwable)
  }
  
  protected def logCustomLevelWithException( level : String, identifier: => String, msg: => String, throwable: Throwable, addHasing : Boolean = true ) {
    try{
     
      val field = Class.forName("org.slf4j.impl.Log4jLoggerAdapter").getDeclaredField("logger")
      field.setAccessible(true)
      val log4jLog = field.get(log).asInstanceOf[ org.apache.log4j.Logger ]
      log4jLog.log(CustomLevel.toLevel( level ), updateLogMessage( msg, identifier, addHasing ) , throwable)
    }catch{
      case ex : Exception => println("")
    }
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  def updateLogMessage ( msg : String , identifier : String, addHash : Boolean ) = updateIdentifier( identifier, addHash ) + LoggingUtils.IDENTIFIER_SEPARATOR + msg
  
  private def updateIdentifier( identifier : String, addHash : Boolean ) : String = addHash match {
    case true => getHash +":"+ identifier
    case _ => identifier
  }
  
  
  private def getHash = {
    val uuID = UUID.fromString( new com.eaio.uuid.UUID().toString ).toString()
    MessageDigest.getInstance( "MD5" ).digest( uuID.getBytes ).map( "%02X".format(_) ).mkString
  }
  

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    if (! Logging.initialized ) {
      Logging.initLock.synchronized {
        if (! Logging.initialized ) {
          initializeLogging(isInterpreter)
        }
      }
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    println("################### binderClass :: "+binderClass)
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals( binderClass )
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        val defaultLogProps = "log4j.properties"  //"kafka-logger.properties"//"org/apache/spark/log4j-defaults.properties"
        //Source.fromFile("UTF-8")
        Option(getClass.getClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>{
            PropertyConfigurator.configure(url)
            System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
          }
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val rootLogger = LogManager.getRootLogger()
        val replLogger = LogManager.getLogger(logName)
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel()) {
          System.err.printf("Setting default log level to \"%s\".\n", replLevel)
          System.err.println("To adjust logging level use sc.setLogLevel(newLevel).")
          rootLogger.setLevel(replLevel)
        }
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
  
  
}

private object Logging {
  @volatile private var initialized = false
  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    val bridgeClass = Class.forName( "org.slf4j.bridge.SLF4JBridgeHandler" )//getClass.getClassLoader.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
}
}

object LoggingUtils{
  val IDENTIFIER_SEPARATOR = "@@SEP@@"
}
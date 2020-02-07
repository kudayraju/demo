package com.hcl.optimus.common.log.appenders

import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util

import com.hcl.optimus.common.log.logger.LoggingUtils
import org.apache.log4j.PatternLayout
import org.apache.log4j.spi.{ErrorCode, LoggingEvent}
import org.json.JSONObject

trait ParseSQLForColumnNames[T] extends (T => List[T])
object ParseSQLForColumnNames extends ParseSQLForColumnNames[String] {
  override def apply( givenSQL: String): List[String] = {
    val subPreSql = givenSQL.substring(givenSQL.indexOf("(")+1)
    val subPostSql = subPreSql.substring(0, subPreSql.indexOf(")")-1 ).trim
    subPostSql.split(",").map( _.trim).toList
  }
}


trait ParseMessageJson[T] extends (T => Map[T, T])
object ParseMessageJson extends ParseMessageJson[String] {
  override def apply( jsonMsgStr: String): Map[String, String] = {
    var map = Map.empty[String, String]
    val json = new JSONObject(jsonMsgStr)
    val jsonItr: util.Iterator[_] = json.keys()
    while(jsonItr.hasNext){
      val key = jsonItr.next()
      val value: AnyRef = json.get(key.toString)
      map = map + ( key.toString -> value.toString )
    }
    map
  }
}

trait MassageMessage[T] extends ((T) => Map[T, T])
object MassageMessage extends MassageMessage[String] {
  override def apply(message: String ): Map[String, String] = {
    message.indexOf( LoggingUtils.IDENTIFIER_SEPARATOR ) == -1 match {
      case true => ParseMessageJson(message)
      case false => val messageList = message.split( LoggingUtils.IDENTIFIER_SEPARATOR ).toList
        ParseMessageJson(messageList(1)).updated("LOGGING_ID", messageList(0))
    }
  }
}

trait CreateSQLValues[T,T1,R] extends ((T,T1) => R)
object CreateSQLValues extends CreateSQLValues[Map[String, String], List[String], String] {
  override def apply(columValueMap: Map[String, String], columnNames : List[String]): String = {
    val stringBuilder = StringBuilder.newBuilder
    columnNames.map{ column =>
      val colValue = columValueMap.getOrElse(column, "")
      stringBuilder.append("'").append(columValueMap.getOrElse(column, "")).append("'").append(",")
    }
    stringBuilder.toString().substring(0, stringBuilder.toString().length-1)
  }
}

trait MassageSQL[T1,T2, R] extends((T1,T2) => R)
object MassageSQL extends MassageSQL[String, String, String] {
  override def apply(sqlValues: String, sql: String): String = {
    val valueStrIndex = sql.toLowerCase().indexOf("values")
    val preSql = sql.substring(0, valueStrIndex+6)
    preSql+" ("+sqlValues+" )"
  }
}


class JDBCAppender extends org.apache.log4j.AppenderSkeleton with com.hcl.optimus.common.utilities.StichFunction{

  protected var columnNames = List.empty[String]
  /**
    * URL of the DB for default connection handling
    */
  protected var databaseURL = "jdbc:odbc:myDB"

  /**
    * User to connect as for default connection handling
    */
  protected var databaseUser = "me"

  /**
    * User to use for default connection handling
    */
  protected var databasePassword = "mypassword"

  /**
    * Connection used by default.  The connection is opened the first time it
    * is needed and then held open until the appender is closed (usually at
    * garbage collection).  This behavior is best modified by creating a
    * sub-class and overriding the <code>getConnection</code> and
    * <code>closeConnection</code> methods.
    */
  protected var connection: Connection = null

  /**
    * Stores the string given to the pattern layout for conversion into a SQL
    * statement, eg: insert into LogTable (Thread, Class, Message) values
    * ("%t", "%c", "%m").
    *
    * Be careful of quotes in your messages!
    *
    * Also see PatternLayout.
    */
  protected var sqlStatement = ""

  /**
    * size of LoggingEvent buffer before writting to the database.
    * Default is 1.
    */
  protected var bufferSize = 1

  /**
    * ArrayList holding the buffer of Logging Events.
    */

  //private  var buffer: util.ArrayList[LoggingEvent] = null

  private  var buffer: util.ArrayList[LoggingEvent] = new util.ArrayList[LoggingEvent](bufferSize)

  /**
    * Helper object for clearing out the buffer
    */
  //protected var removes: util.ArrayList[LoggingEvent] = null
  protected var removes: util.ArrayList[LoggingEvent] = new util.ArrayList[LoggingEvent](bufferSize)

  private var locationInfo = false

  /*def this() {
    this()
    //super()
    buffer = new util.ArrayList[LoggingEvent](bufferSize)
    removes = new util.ArrayList[LoggingEvent](bufferSize)
  }*/

  /**
    * Gets whether the location of the logging request call
    * should be captured.
    *
    * @since 1.2.16
    * @return the current value of the <b>LocationInfo</b> option.
    */
  def getLocationInfo: Boolean = locationInfo

  /**
    * The <b>LocationInfo</b> option takes a boolean value. By default, it is
    * set to false which means there will be no effort to extract the location
    * information related to the event. As a result, the event that will be
    * ultimately logged will likely to contain the wrong location information
    * (if present in the log format).
    * <p/>
    * <p/>
    * Location information extraction is comparatively very slow and should be
    * avoided unless performance is not a concern.
    * </p>
    *
    * @since 1.2.16
    * @param flag true if location information should be extracted.
    */
  def setLocationInfo(flag: Boolean): Unit = {
    locationInfo = flag
  }


  /**
    * Adds the event to the buffer.  When full the buffer is flushed.
    */
  override def append(event: LoggingEvent): Unit = {
    event.getNDC
    event.getThreadName
    // Get a copy of this thread's MDC.
    event.getMDCCopy()
    if (locationInfo) event.getLocationInformation
    event.getRenderedMessage
    event.getThrowableStrRep
    //buffer.
    buffer.add(event)
    if (buffer.size >= bufferSize) flushBuffer()
  }

  /**
    * By default getLogStatement sends the event to the required Layout object.
    * The layout will format the given pattern into a workable SQL string.
    *
    * Overriding this provides direct access to the LoggingEvent
    * when constructing the logging statement.
    *
    */
  protected def getLogStatement(event: LoggingEvent): String = {
    //getLayout.format(event)
    MassageMessage(event.getMessage.toString )  >=< (CreateSQLValues(_, columnNames)) >=< (MassageSQL(_, sqlStatement))
  }

  /**
    *
    * Override this to provide an alertnate method of getting
    * connections (such as caching).  One method to fix this is to open
    * connections at the start of flushBuffer() and close them at the
    * end.  I use a connection pool outside of JDBCAppender which is
    * accessed in an override of this method.
    **/
  @throws[SQLException]
  protected def execute(sql: String): Unit = {
    var con : Connection = null
    var stmt : Statement = null
    try {
      con = getConnection
      stmt = con.createStatement
      stmt.executeUpdate(sql)
    } finally {
      if (stmt != null) stmt.close()
      closeConnection(con)
    }
  }


  /**
    * Override this to return the connection to a pool, or to clean up the
    * resource.
    *
    * The default behavior holds a single connection open until the appender
    * is closed (typically when garbage collected).
    */
  protected def closeConnection(con: Connection): Unit = {
  }

  /**
    * Override this to link with your connection pooling system.
    *
    * By default this creates a single connection which is held open
    * until the object is garbage collected.
    */
  @throws[SQLException]
  protected def getConnection: Connection = {
    if (!DriverManager.getDrivers.hasMoreElements) setDriver("sun.jdbc.odbc.JdbcOdbcDriver")
    if (connection == null) connection = DriverManager.getConnection(databaseURL, databaseUser, databasePassword)
    connection
  }

  /**
    * Closes the appender, flushing the buffer first then closing the default
    * connection if it is open.
    */
  override def close(): Unit = {
    flushBuffer()
    try
        if (connection != null && !connection.isClosed) connection.close()
    catch {
      case e: SQLException =>
        errorHandler.error("Error closing connection", e, ErrorCode.GENERIC_FAILURE)
    }
    this.closed = true
  }

  /**
    * loops through the buffer of LoggingEvents, gets a
    * sql string from getLogStatement() and sends it to execute().
    * Errors are sent to the errorHandler.
    *
    * If a statement fails the LoggingEvent stays in the buffer!
    */
  def flushBuffer(): Unit = { //Do the actual logging
    removes.ensureCapacity(buffer.size)
    val i = buffer.iterator
    while ( {
      i.hasNext
    }) {
      val logEvent = i.next.asInstanceOf[LoggingEvent]
      try {
        val sql = getLogStatement(logEvent)
        execute(sql)
      } catch {
        case e: SQLException =>
          errorHandler.error("Failed to excute sql", e, ErrorCode.FLUSH_FAILURE)
      } finally removes.add(logEvent)
    }
    // remove from the buffer any events that were reported
    buffer.removeAll(removes)
    // clear the buffer of reported events
    removes.clear()
  }


  /** closes the appender before disposal */
  override def finalize(): Unit = {
    close()
  }


  /**
    * JDBCAppender requires a layout.
    **/
  override def requiresLayout = true


  /**
    *
    */
  def setSql(s: String): Unit = {
    sqlStatement = s
    println("############## setSql :: "+sqlStatement)
    columnNames = sqlStatement >=< ParseSQLForColumnNames
    if (getLayout == null) this.setLayout(new PatternLayout(s))
    else getLayout.asInstanceOf[PatternLayout].setConversionPattern(s)
  }


  /**
    * Returns pre-formated statement eg: insert into LogTable (msg) values ("%m")
    */
  def getSql: String = sqlStatement


  def setUser(user: String): Unit = {
    databaseUser = user
  }


  def setURL(url: String): Unit = {
    databaseURL = url
  }


  def setPassword(password: String): Unit = {
    println("############ setPassword :: "+password)
    databasePassword = com.hcl.optimus.common.utilities.Decrypt( password )
  }


  def setBufferSize(newBufferSize: Int): Unit = {
    bufferSize = newBufferSize
    buffer.ensureCapacity(bufferSize)
    removes.ensureCapacity(bufferSize)
  }


  def getUser: String = databaseUser


  def getURL: String = databaseURL


  def getPassword: String = databasePassword


  def getBufferSize: Int = bufferSize


  /**
    * Ensures that the given driver class has been loaded for sql connection
    * creation.
    */
  def setDriver(driverClass: String): Unit = {
    try
      Class.forName(driverClass)
    catch {
      case e: Exception =>
        errorHandler.error("Failed to load driver", e, ErrorCode.GENERIC_FAILURE)
    }
  }

  /*override def append(event: LoggingEvent): Unit = {
    println("################## RenderedMessage"+event.getRenderedMessage)
    println("################## Properties"+event.getProperties)
  }

  override def close(): Unit = ???

  override def requiresLayout(): Boolean = ???*/
}

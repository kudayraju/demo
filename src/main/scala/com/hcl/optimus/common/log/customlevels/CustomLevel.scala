package com.hcl.optimus.common.log.customlevels

import org.apache.log4j.Level

object CustomLevel {
  
  val DP_ERROR = "DPERROR"
  val AUDIT_LOG = "AUDITLOG"
  val LINEAGE_LOG = "LINEAGELOG"
  val ROCS_ERROR = "ROCSERROR"
  val LEVEL_1 = "LEVEL1"
  val LEVEL_2 = "LEVEL2"
  val LEVEL_3 = "LEVEL3"
  
  val DPERROR = new CustomLevel( 50010, DP_ERROR, 10)
  val ROCSERROR = new CustomLevel( 50015, ROCS_ERROR, 10)
  val AUDITLOG = new CustomLevel( 50020, AUDIT_LOG, 10)
  val LINEAGELOG = new CustomLevel( 50025, LINEAGE_LOG, 10)
  val LEVEL1 = new CustomLevel( 50035, LEVEL_1, 10)
  val LEVEL2 = new CustomLevel( 50045, LEVEL_2, 10)
  val LEVEL3 = new CustomLevel( 50055, LEVEL_3, 10)
  
  def toLevel( levelString : String ) = levelString match {
    case DP_ERROR => DPERROR
    case ROCS_ERROR => ROCSERROR
	  case AUDIT_LOG => AUDITLOG
    case LINEAGE_LOG => LINEAGELOG
    case LEVEL_1 => LEVEL1
    case LEVEL_2 => LEVEL2
    case LEVEL_3 => LEVEL3
    case _ => Level.INFO
  }
  
  def toLevel( levelString : String, defaultLevel : Level ) = levelString match {
    case DP_ERROR => DPERROR
    case ROCS_ERROR => ROCSERROR
	  case AUDIT_LOG => AUDITLOG
    case LINEAGE_LOG => LINEAGELOG
    case LEVEL_1 => LEVEL1
    case LEVEL_2 => LEVEL2
    case LEVEL_3 => LEVEL3
    case _ => defaultLevel
  }
  
  def toLevel( value : Int, defaultLevel : Level ) = value match {
    case 50010 => DPERROR
    case 50015 => ROCSERROR
    case 50020 => AUDITLOG
    case 50025 => LINEAGELOG
    case 50035 => LEVEL1
    case 50045 => LEVEL2
    case 50055 => LEVEL3
    case _ => defaultLevel
  }
  
}

class CustomLevel(level : Int, levelString : String, sysLogEqivalent : Int ) extends Level(level, levelString, sysLogEqivalent){}
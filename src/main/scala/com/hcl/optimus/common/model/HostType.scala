package com.hcl.optimus.common.model

object HostType extends Enumeration {
  type HostType = Value

  val EDGENODE = Value(1, "EDGENODE")
  val FTP = Value(2, "FTP")
  val SFTP = Value(3, "SFTP")
  val HADOOP = Value(4, "HADOOP")
}

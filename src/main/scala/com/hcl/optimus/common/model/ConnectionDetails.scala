package com.hcl.optimus.common.model
import com.hcl.optimus.common.model.HostType._

case class ConnectionDetails (val host: String,
                              val port: Int,
                              val user: String,
                              val password: String,
                              val hostType: HostType)

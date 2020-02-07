package com.hcl.optimus.common.model

case class SFTPConnectionDetails (val host: String,
                                 val port: Int,
                                 val user: String,
                                 val password: String)
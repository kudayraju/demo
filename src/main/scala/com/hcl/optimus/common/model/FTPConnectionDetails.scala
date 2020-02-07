package com.hcl.optimus.common.model

case class FTPConnectionDetails (val host: String,
                                 val port: Int,
                                 val user: String,
                                 val password: String)
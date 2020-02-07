package com.hcl.optimus.common.model

import org.apache.spark.sql.SparkSession

case class AzureBlobAuthInfo(
                              sparkSession: SparkSession,
                              extraConfig: Map[String, String],
                              mountPoint:String,
                              clientId: String,
                              clientCredential: String,
                              clientRefreshURL: String,
                              storageAccountName: String,
                              storageAccountKeyName: String,
                              storageAccountAccessKey: String,
                              storageContainerName: String,
                              storageRootURL: String,
                              accountFQDN: String,
                              isKeyVault: Boolean
                            )

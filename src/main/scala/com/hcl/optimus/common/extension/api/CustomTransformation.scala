package com.hcl.optimusclient.extension.api

import org.apache.spark.sql.DataFrame

trait CustomTransformation {
  def doTransform(inputDataFrame : DataFrame): DataFrame
}

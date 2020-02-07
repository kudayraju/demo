package com.hcl.optimus.common

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.ivy.util.StringUtils

object SampleSpark {
  val sparkConf = new SparkConf().setAppName("SAMPLEJOB")
  val sparkContext = new SparkContext(sparkConf)
  val data = Array("abc", "sanjib", "acb", "xyz", "yzx")
  val rdd = sparkContext.parallelize(data)
  val tupleRdd = rdd.map { str => (/*StringUtils.Sort(str)*/str, str)}.groupBy { x => x._1 }
  
}
package com.hcl.optimusclient.extension.api

import org.apache.spark.sql.DataFrame

object TransformationExtensionStub {

  implicit class _TransformationExtensionStub(inputDataFrame: DataFrame) {
    /*def transformBy(transformationFunc: (DataFrame => DataFrame)): DataFrame = {
      transformationFunc(inputDataFrame)
    }*/
    /*def transformBy(fullyClassifiedTransformerClassName: String, transformationMethodName: String): DataFrame = {
      import scala.reflect.runtime.universe._
      val mirror: Mirror = runtimeMirror(Class.forName(fullyClassifiedTransformerClassName).getClassLoader)
      val instanceMirror = mirror.reflect(Class.forName(fullyClassifiedTransformerClassName).newInstance())

      val methodSymbol = instanceMirror.symbol.toType.decl(TermName(transformationMethodName)).asMethod
      val methodMirror = instanceMirror.reflectMethod(methodSymbol)
      methodMirror.apply(inputDataFrame).asInstanceOf[DataFrame]
    }*/
    def transformBy(func: scala.reflect.runtime.universe.MethodMirror) : DataFrame = {
      func.apply(inputDataFrame).asInstanceOf[DataFrame]
    }

  }
}


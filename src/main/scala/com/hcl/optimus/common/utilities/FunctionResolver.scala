package com.hcl.optimus.common.utilities

object FunctionResolver {
  import scala.reflect.runtime.universe._
  val resolveFunction: (String, String) => MethodMirror = (className: String, functionName: String) => {
    // get runtime mirror
    val mirror: Mirror = runtimeMirror(Class.forName(className).getClassLoader)
    // get instance mirror
    val instanceMirror = mirror.reflect(Class.forName(className).newInstance())
    // get method symbol
    val methodSymbol = instanceMirror.symbol.toType.decl(TermName(functionName)).asMethod
    //get method mirror
    val myMethod = instanceMirror.reflectMethod(methodSymbol)
    myMethod
  }


}

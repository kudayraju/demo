package com.hcl.optimus.common.utilities

trait StichFunction {

  implicit class StichFunctionPimper[T]( t : T){

    def >=<[T1]( fn : T => T1 ) : T1 ={
      fn(t)
    }

  }

}

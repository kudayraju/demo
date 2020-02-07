package com.hcl.optimus.common.utilities

trait Arrow {

  implicit class ArrowPimper[T](t: T) {
    def ~>[T1](fn: T => T1) = fn(t)
  }

}

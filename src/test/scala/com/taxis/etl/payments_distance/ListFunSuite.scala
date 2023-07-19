package com.taxis.etl.payments_distance

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ListFunSuite extends AnyFunSuite {

  test("Calculate rate should return false") {
    assert(!PaymentsDistance.calculateRate(null))
  }

}

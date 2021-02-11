package com.github.sadikovi

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** abstract general testing class */
abstract class UnitTestSuite extends AnyFunSuite
  with Matchers with BeforeAndAfterAll with BeforeAndAfter

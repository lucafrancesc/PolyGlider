package com.polyglider.consumer

import munit.CatsEffectSuite
import com.polyglider.consumer.ProcessingFailure.{PermanentFailure, TransientFailure}

class ProcessingFailureSuite extends CatsEffectSuite {
  test("circe parsing failure is classified as permanent") {
    val err = io.circe.parser.parse("{ not valid json }").swap.getOrElse(fail("expected a parsing failure"))
    assertEquals(ProcessingFailure.classify(err), PermanentFailure(err))
  }

  test("circe decoding failure is classified as permanent") {
    val err = io.circe.Json.obj().as[Int].swap.getOrElse(fail("expected a decoding failure"))
    assertEquals(ProcessingFailure.classify(err), PermanentFailure(err))
  }

  test("postgres unique violation (SQLState 23505) is classified as permanent") {
    val err = new java.sql.SQLException("duplicate key value violates unique constraint", "23505")
    assertEquals(ProcessingFailure.classify(err), PermanentFailure(err))
  }

  test("postgres connection exception (SQLState 08006) is classified as transient") {
    val err = new java.sql.SQLException("connection failure", "08006")
    assertEquals(ProcessingFailure.classify(err), TransientFailure(err))
  }

  test("SQLTransientException is classified as transient") {
    val err = new java.sql.SQLTransientException("timeout")
    assertEquals(ProcessingFailure.classify(err), TransientFailure(err))
  }

  test("generic IOException is classified as transient") {
    val err = new java.io.IOException("broker unreachable")
    assertEquals(ProcessingFailure.classify(err), TransientFailure(err))
  }

  test("TimeoutException is classified as transient") {
    val err = new java.util.concurrent.TimeoutException("timed out")
    assertEquals(ProcessingFailure.classify(err), TransientFailure(err))
  }

  test("unrecognized exception defaults to permanent") {
    val err = new RuntimeException("business rule violation")
    assertEquals(ProcessingFailure.classify(err), PermanentFailure(err))
  }
}

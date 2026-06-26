package com.polyglider

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class AppConfigSpec extends FunSuite {

  test("AppConfig.load succeeds with the default application.conf") {
    val result = AppConfig.load(ConfigFactory.load())
    result match {
      case Right(cfg) =>
        assertEquals(cfg.workers, 4)
        assertEquals(cfg.queueSize, 1000)
        assertEquals(cfg.summaryEvery, 10L)
        assertEquals(cfg.retryPolicy.maxRetries, 5)
        assertEquals(cfg.reprocessorPolicy.maxRetries, 3)
        assertEquals(cfg.circuitBreakerMaxFailures, 3)
        assertEquals(cfg.metricsPort, 9100)
      case Left(err) =>
        fail(s"Expected Right but got Left: $err")
    }
  }

  test("AppConfig.load returns Left with a descriptive message for a malformed integer value") {
    val conf = ConfigFactory.parseString("""
      app.consumer.workers = "notanumber"
    """).withFallback(ConfigFactory.load())
    val result = AppConfig.load(conf)
    assert(result.isLeft, "expected Left for malformed int")
    result.left.foreach { msg =>
      assert(msg.contains("app.consumer.workers"), s"error message should name the bad key, got: $msg")
    }
  }

  test("AppConfig.load accumulates multiple errors") {
    val conf = ConfigFactory.parseString("""
      app.consumer.workers = "bad"
      app.metrics.port = "alsobad"
    """).withFallback(ConfigFactory.load())
    val result = AppConfig.load(conf)
    assert(result.isLeft, "expected Left for multiple malformed values")
    result.left.foreach { msg =>
      assert(msg.contains("app.consumer.workers"), s"expected workers error in: $msg")
      assert(msg.contains("app.metrics.port"), s"expected port error in: $msg")
    }
  }

  test("AppConfig.load falls back to documented defaults for optional fields missing from config") {
    val conf = ConfigFactory.parseString("""
      app.rabbitmq.port = 5672
    """)
    val result = AppConfig.load(conf)
    result match {
      case Right(cfg) =>
        assertEquals(cfg.workers, 4)
        assertEquals(cfg.queueSize, 1000)
        assertEquals(cfg.summaryEvery, 10L)
        assertEquals(cfg.metricsPort, 9100)
        assertEquals(cfg.runMigrations, true)
      case Left(err) =>
        fail(s"Expected Right with defaults, got Left: $err")
    }
  }
}

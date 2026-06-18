package com.polyglider

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import doobie.*
import doobie.implicits.*
import munit.CatsEffectSuite
import org.flywaydb.core.Flyway
import org.testcontainers.utility.DockerImageName
import com.polyglider.storage.{DoobieSkuStorage, SkuStats}

import java.util.UUID

/** Exercises the real DoobieSkuStorage against a real Postgres instance via Testcontainers,
  * running the actual Flyway migrations from src/main/resources/db/migration. DatabaseSpec
  * tests H2-compatible SQL that reimplements the upsert logic by hand — H2 doesn't support the
  * Postgres `ON CONFLICT` syntax DoobieSkuStorage actually uses, so it was never exercising the
  * production class or catching bugs in its real SQL (see #21, where DoobieSkuStorage's dedup
  * insert had a bug that none of the H2-based tests could have caught).
  */
class DoobieSkuStorageSpec extends CatsEffectSuite {
  private val container = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))
  private var xa: Transactor[IO] = _
  private var storage: DoobieSkuStorage = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    Flyway.configure()
      .dataSource(container.jdbcUrl, container.username, container.password)
      .load()
      .migrate()
    xa = Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = container.jdbcUrl,
      user = container.username,
      password = container.password,
      logHandler = None
    )
    storage = new DoobieSkuStorage(xa)
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  private def freshSku(): String = s"SKU-${UUID.randomUUID()}"
  private def freshEventId(): String = UUID.randomUUID().toString

  private def statsFor(sku: String): IO[Option[SkuStats]] =
    storage.snapshot.map(_.find(_.sku == sku))

  test("upsertSku inserts a new SKU with qty and order_count of 1") {
    val sku = freshSku()
    for {
      _    <- storage.upsertSku(freshEventId(), sku, 5)
      stat <- statsFor(sku)
    } yield assertEquals(stat, Some(SkuStats(sku, 5, 1)))
  }

  test("upsertSku accumulates qty and order_count across multiple events for the same SKU") {
    val sku = freshSku()
    for {
      _    <- storage.upsertSku(freshEventId(), sku, 5)
      _    <- storage.upsertSku(freshEventId(), sku, 3)
      _    <- storage.upsertSku(freshEventId(), sku, 10)
      stat <- statsFor(sku)
    } yield assertEquals(stat, Some(SkuStats(sku, 18, 3)))
  }

  test("redelivering the same eventId does not double-count the ledger (see #21)") {
    val sku = freshSku()
    val eventId = freshEventId()
    for {
      _    <- storage.upsertSku(eventId, sku, 5)
      _    <- storage.upsertSku(eventId, sku, 5) // redelivery of the same event
      stat <- statsFor(sku)
    } yield assertEquals(stat, Some(SkuStats(sku, 5, 1)), "duplicate eventId must be a no-op, not a second application")
  }

  test("snapshot returns rows ordered by sku") {
    val a = s"A-${UUID.randomUUID()}"
    val b = s"B-${UUID.randomUUID()}"
    for {
      _    <- storage.upsertSku(freshEventId(), b, 1)
      _    <- storage.upsertSku(freshEventId(), a, 1)
      snap <- storage.snapshot
      relevant = snap.filter(s => s.sku == a || s.sku == b)
    } yield assertEquals(relevant.map(_.sku), List(a, b))
  }

  test("a negative delta that would drive qty below zero is rejected by the CHECK constraint") {
    val sku = freshSku()
    for {
      _      <- storage.upsertSku(freshEventId(), sku, 5)
      result <- storage.upsertSku(freshEventId(), sku, -10).attempt
    } yield assert(result.isLeft, "qty going negative must be rejected, not silently stored")
  }
}

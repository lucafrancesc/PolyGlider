package com.polyglider

import cats.effect.IO
import com.dimafeng.testcontainers.PostgreSQLContainer
import doobie.*
import munit.CatsEffectSuite
import org.flywaydb.core.Flyway
import org.testcontainers.utility.DockerImageName
import com.polyglider.storage.{DoobieSkuStorage, SkuStats}

import java.util.UUID

/** Verifies that concurrent upserts to the same SKU from multiple fibers produce the correct
  * final quantity. The upsertSku implementation uses a single `INSERT … ON CONFLICT … DO UPDATE`
  * that increments in the database, so concurrent writers should never lose an update the way a
  * read-modify-write at the application layer would. These tests confirm that property holds
  * against a real Postgres instance.
  */
class ConcurrentStorageSpec extends CatsEffectSuite {
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

  private def freshSku(): String     = s"SKU-${UUID.randomUUID()}"
  private def freshEventId(): String = UUID.randomUUID().toString

  private def statsFor(sku: String): IO[Option[SkuStats]] =
    storage.snapshot.map(_.find(_.sku == sku))

  test("concurrent upserts to the same SKU produce the correct final quantity") {
    val n   = 20
    val sku = freshSku()
    val eventIds = List.fill(n)(freshEventId())
    val tasks = eventIds.map(id => storage.upsertSku(id, sku, 1))
    for {
      _    <- IO.parSequenceN(4)(tasks)
      stat <- statsFor(sku)
    } yield assertEquals(stat, Some(SkuStats(sku, n.toLong, n.toLong)), s"all $n increments must land")
  }

  test("concurrent upserts with one duplicate eventId do not double-count") {
    val n        = 20
    val sku      = freshSku()
    val distinct = List.fill(n)(freshEventId())
    // replace the last slot with a repeat of the first — 19 distinct events
    val withDupe = distinct.init :+ distinct.head
    val tasks    = withDupe.map(id => storage.upsertSku(id, sku, 1))
    for {
      _    <- IO.parSequenceN(4)(tasks)
      stat <- statsFor(sku)
    } yield assertEquals(stat, Some(SkuStats(sku, (n - 1).toLong, (n - 1).toLong)), "duplicate eventId must not be counted twice")
  }
}

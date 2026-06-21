package com.polyglider.storage

import cats.effect.IO

case class SkuStats(sku: String, qty: Long, orderCount: Long)

enum UpsertResult:
  case Applied, DuplicateSkipped

trait SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[UpsertResult]
  def snapshot: IO[List[SkuStats]]
}

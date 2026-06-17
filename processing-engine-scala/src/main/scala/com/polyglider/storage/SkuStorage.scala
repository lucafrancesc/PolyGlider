package com.polyglider.storage

import cats.effect.IO

case class SkuStats(sku: String, qty: Long, orderCount: Long)

trait SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[Unit]
  def snapshot: IO[List[SkuStats]]
}

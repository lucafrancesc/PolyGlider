package com.polyglider.storage

import cats.effect.IO

trait SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[Unit]
}

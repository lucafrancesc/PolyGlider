package com.polyglider.model

import io.circe._

case class OrderPlaced(eventId: String, sku: String, quantity: Int, customerId: String, timestamp: String, version: String = "1")

object OrderPlaced {
  // Messages published before this field existed (and anything constructing the payload by
  // hand, e.g. tools/chaos/publish_chaos_message.py) simply omit `version` -- treating that
  // absence as "1" rather than failing to decode keeps every pre-existing payload valid,
  // consistent with the additive-change compatibility rule in ADR-006.
  implicit val decoder: Decoder[OrderPlaced] = (c: HCursor) =>
    for {
      eventId    <- c.get[String]("eventId")
      sku        <- c.get[String]("sku")
      quantity   <- c.get[Int]("quantity")
      customerId <- c.get[String]("customerId")
      timestamp  <- c.get[String]("timestamp")
      version    <- c.getOrElse[String]("version")("1")
    } yield OrderPlaced(eventId, sku, quantity, customerId, timestamp, version)
}

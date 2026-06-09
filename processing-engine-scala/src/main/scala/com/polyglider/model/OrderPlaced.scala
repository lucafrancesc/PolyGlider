package com.polyglider.model

import io.circe._
import io.circe.generic.auto._

case class OrderPlaced(eventId: String, sku: String, quantity: Int, customerId: String, timestamp: String)

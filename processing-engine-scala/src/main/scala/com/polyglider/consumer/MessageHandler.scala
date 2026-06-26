package com.polyglider.consumer

import cats.effect.IO
import io.circe.parser
import com.polyglider.model.OrderPlaced
import com.polyglider.storage.{SkuStorage, UpsertResult}

import java.nio.charset.StandardCharsets

trait MessageHandler[A] {
  def decode(body: Array[Byte]): Either[Throwable, A]
  def validate(a: A): Either[Throwable, A]
  def eventIdOf(body: Array[Byte]): String
  def process(a: A): IO[UpsertResult]
}

class OrderPlacedHandler(storage: SkuStorage) extends MessageHandler[OrderPlaced] {
  def decode(body: Array[Byte]): Either[Throwable, OrderPlaced] =
    parser.parse(new String(body, StandardCharsets.UTF_8))
      .flatMap(_.as[OrderPlaced])
      .left.map(identity)

  def validate(order: OrderPlaced): Either[Throwable, OrderPlaced] =
    RabbitConsumer.validateUuids(order).flatMap(RabbitConsumer.validateVersion)

  def eventIdOf(body: Array[Byte]): String =
    RabbitConsumer.eventIdOf(body)

  def process(order: OrderPlaced): IO[UpsertResult] =
    storage.upsertSku(order.eventId, order.sku, order.quantity)
}

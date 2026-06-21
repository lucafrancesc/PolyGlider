package com.polyglider

import munit.CatsEffectSuite
import cats.effect.IO
import io.circe.parser.*
import io.circe.generic.auto.*
import com.polyglider.model.OrderPlaced

import au.com.dius.pact.consumer.MessagePactBuilder
import au.com.dius.pact.consumer.dsl.PactDslJsonBody
import au.com.dius.pact.core.model.PactSpecVersion
import au.com.dius.pact.core.model.messaging.MessagePact

import java.nio.file.{Files, Paths}

class OrderPlacedEventContractSpec extends CatsEffectSuite {

  private val exampleEventId    = "11111111-1111-4111-8111-111111111111"
  private val exampleCustomerId = "22222222-2222-4222-8222-222222222222"
  private val exampleTimestamp  = "2024-01-15T10:30:00Z"

  test("defines pact for OrderPlaced message and Scala consumer can deserialize it") {
    val pactDir = "../contracts/pacts"

    val body = new PactDslJsonBody()
      .uuid("eventId", exampleEventId)
      .stringType("sku", "SKU-001")
      .integerType("quantity", Integer.valueOf(5))
      .uuid("customerId", exampleCustomerId)
      .stringType("timestamp", exampleTimestamp)
      .stringType("version", "1")

    val pact: MessagePact = new MessagePactBuilder()
      .consumer("scala-engine")
      .hasPactWith("cs-gateway")
      .expectsToReceive("an order placed event")
      .withContent(body)
      .toPact()

    for {
      _ <- IO.blocking {
        Files.createDirectories(Paths.get(pactDir))
        val _ = pact.write(pactDir, PactSpecVersion.V3)
      }

      // Verify the consumer can deserialize a representative payload of the
      // same shape as defined in the pact body above.
      exampleJson =
        s"""{
           |  "eventId": "$exampleEventId",
           |  "sku": "SKU-001",
           |  "quantity": 5,
           |  "customerId": "$exampleCustomerId",
           |  "timestamp": "$exampleTimestamp",
           |  "version": "1"
           |}""".stripMargin

      result <- IO(parse(exampleJson).flatMap(_.as[OrderPlaced]))
      _ <- IO {
        assert(result.isRight, s"consumer failed to deserialize agreed message shape: $result")
        val order = result.toOption.get
        assertEquals(order.eventId, exampleEventId)
        assertEquals(order.sku, "SKU-001")
        assertEquals(order.quantity, 5)
        assertEquals(order.customerId, exampleCustomerId)
        assertEquals(order.timestamp, exampleTimestamp)
        assertEquals(order.version, "1")
      }
    } yield ()
  }

  test("consumer deserializes a pre-#41 payload with no version field, defaulting to \"1\"") {
    val exampleJson =
      s"""{
         |  "eventId": "$exampleEventId",
         |  "sku": "SKU-001",
         |  "quantity": 5,
         |  "customerId": "$exampleCustomerId",
         |  "timestamp": "$exampleTimestamp"
         |}""".stripMargin

    val result = parse(exampleJson).flatMap(_.as[OrderPlaced])
    assert(result.isRight, s"consumer failed to deserialize a pre-existing 5-field payload: $result")
    assertEquals(result.toOption.get.version, "1")
  }
}

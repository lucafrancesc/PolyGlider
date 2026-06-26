package com.polyglider.consumer

import com.rabbitmq.client.{AMQP, Channel}

// Shared across RabbitConsumer and DlqReprocessor; private[polyglider] so tests in com.polyglider
// can reference the type directly without going through either object.
private[polyglider] case class Delivery(
  channel: Channel,
  deliveryTag: Long,
  body: Array[Byte],
  properties: AMQP.BasicProperties = new AMQP.BasicProperties()
)
